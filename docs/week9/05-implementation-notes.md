# 05. 구현 노트 — 실시간 랭킹

구현 시점의 설계 트레이드오프와 파일 맵을 기록한다. 요구사항은 [`01-requirements.md`](./01-requirements.md), 데이터 모델은 [`04-redis-model.md`](./04-redis-model.md) 참조.

> **상태**: Must-Have 구현 완료(branch `volume-9`). Nice-to-Have(시간 단위 랭킹·콜드 스타트)는 미포함. 단위·통합 테스트 작성 완료, **Docker 환경에서 전부 green**. **수동 E2E 검증 완료**(2026-07-17) — 이벤트 발행 → Kafka 컨슘 → ZSET → API 노출까지 실측했고, 그 과정에서 발견한 중복 발행 문제로 §2.1의 멱등 설계를 개정했다.

---

## 1. 파일 맵

### commerce-streamer (적재/쓰기)

| 파일 | 역할 |
| --- | --- |
| `interfaces/consumer/ProductRankingConsumer` | `@KafkaListener` 배치, 그룹 `ranking-aggregator`, 수동 커밋 |
| `application/ranking/RankingAggregator` | 배치 파싱 + `(키, productId)` 델타 coalescing + `event_handled` 멱등(§2.1) |
| `application/ranking/RankingScorePolicy` | 가중치 상수 + 스코어 계산(view/like/order) |
| `infrastructure/ranking/RankingKey` | 키 포맷 + TTL + `occurredAt`→KST 일자 환산 |
| `infrastructure/ranking/RankingRedisRepository` | 파이프라인 `ZINCRBY` + `EXPIRE` |

### commerce-api (조회/읽기)

| 파일 | 역할 |
| --- | --- |
| `interfaces/api/ranking/RankingV1Controller` | `GET /api/v1/rankings` (date/page/size) |
| `interfaces/api/ranking/RankingV1Dto` | 응답 DTO(`RankingPageResponse`/`RankedItem`) |
| `application/ranking/RankingFacade` | 순위+상품 요약 조립(N+1 회피) |
| `application/ranking/RankingPageInfo`·`RankedProductInfo` | 응용 DTO |
| `domain/ranking/RankingRepository` | 조회 포트(인터페이스) |
| `domain/ranking/RankedProduct` | 순위·상품·스코어 레코드 |
| `infrastructure/ranking/RankingRedisRepository` | ZSET `ZREVRANGE`/`ZCARD`/`ZREVRANK` |
| `infrastructure/ranking/RankingKey` | 키 포맷 + `today()`(KST) |

### 변경분(기존 파일)

| 파일 | 변경 |
| --- | --- |
| `infrastructure/metrics/EventHandledRepository`(streamer) | 변경 없음 — `consumer_group` 파라미터 덕에 그대로 재사용(§2.1) |
| `application/outbox/OutboxImmediatePublisher`(api) | 주석 정정 — "브로커 멱등이 중복을 흡수한다"는 서술이 사실과 달라 삭제(§2.1) |
| `domain/order/OrderPaidEvent`(api) | `Item`에 `unitPrice` 추가(원본 주석 보존) |
| `application/product/ProductFacade`(api) | `RankingRepository` 주입, 상세에 `rank` 조합 |
| `application/product/ProductDetailInfo`(api) | `rank` 필드 추가 |
| `application/product/CachedProductDetail`(api) | `toInfo(liked, rank)`(원본 주석 보존) |
| `interfaces/api/product/ProductV1Dto`(api) | `ProductDetailResponse`에 `rank` 추가 |

---

## 2. 설계 트레이드오프

### 2.1 멱등성 — `event_handled` 재사용 (2026-07-17 개정)

> **개정 이력**: 최초 구현은 `event_handled` 없이 "근사 허용"으로 갔다. 근거는 *"`ZINCRBY`는 멱등이 아니라 **컨슈머 재전달 시** 소폭 이중 가산된다 → 랭킹은 근사값이니 수용"* 이었다. **E2E가 이 근거를 반증했다** — 중복의 지배적 출처는 컨슈머 재전달이 아니라 **프로듀서 측 중복 발행**이고, "소폭"이 아니었다.

**E2E에서 관측된 것** (13개 이벤트를 발생시킨 시시한 시나리오):

- 좋아요 취소 이벤트(`eventId=12`)가 `catalog-events`에 **레코드 2개로 적재**됨. outbox 행은 1개뿐 → 중복 INSERT가 아니라 **같은 행을 두 번 발행**한 것.
- 원인: `OutboxImmediatePublisher`(afterCommit 즉시 발행)와 `OutboxRelay`(1초 폴링)가 **같은 행을 잠그지 않는 read-then-act** 라, 커밋 직후 폴링 주기가 겹치면 둘 다 `PENDING`을 보고 각자 보낸다. 해당 행만 `created_at`→`sent_at` 간격이 134ms로 튀어 경합 흔적이 남았다(정상 ~20ms).
- 결과: `product_metrics`는 `event_handled`로 걸러 정상(like_count=0). **랭킹만 −0.2가 이중 반영**되어, 조회1+좋아요+취소 상품이 `+0.1`이어야 할 자리에 `−0.1`을 받았다.

**왜 브로커 멱등으로 안 막히나**: `OutboxImmediatePublisher`의 옛 주석은 "브로커 멱등(`enable.idempotence`)이 중복을 흡수한다"고 적고 있었으나 **사실이 아니다**. `enable.idempotence`는 **한 프로듀서 세션 내 재시도**를 PID+시퀀스 번호로 중복 제거할 뿐이라, 서로 다른 트랜잭션에서 나간 별개의 `send()` 두 번은 서로 다른 레코드로 그대로 적재된다. 중복을 흡수하는 건 **소비자 멱등(`event_handled`)뿐**이다. (주석도 함께 정정했다.)

**그래서 채택한 것**: `EventHandledRepository`는 이미 `consumer_group`을 파라미터로 받으므로 `ranking-aggregator` 그룹으로 그대로 재사용한다 — `metrics-aggregator`와 동일한 2단 방어.

1. 배치 내 중복 `eventId` 제거(먼저 온 것 유지) — 같은 배치에 실려온 중복을 접는다.
2. `findHandled`로 이미 처리한 `eventId` 필터 — 다른 배치로 온 중복을 막는다.
3. Redis 반영 → **같은 트랜잭션에서** `markHandled`.

**남는 창(window)**: Redis와 DB는 한 트랜잭션으로 묶을 수 없다. Redis 반영 후 마킹 커밋 전에 죽으면 재전달 시 **그 배치만** 이중 가산된다(유실은 없음 — at-least-once). 이 잔여 오차는 근사 + 2일 TTL 리셋으로 수용한다. 마킹을 먼저 커밋하는 반대 순서는 이중 가산 대신 **유실**이라 더 나쁘다 — 랭킹에서 없는 점수는 되찾을 방법이 없지만, 이중 가산은 TTL이 지우기 때문이다.

**비용**: 배치당 `SELECT` 1회 + `INSERT` 배치 1회가 추가된다. NFR-2(파이프라인 분리)는 유지된다 — 컨슈머 그룹과 오프셋은 여전히 독립이고, `event_handled`는 그룹별 행이라 `metrics-aggregator`와 경합하지 않는다.

### 2.2 파이프라인 분리 — 별도 컨슈머 그룹

랭킹 적재를 기존 `metrics-aggregator`에 얹지 않고 **별도 그룹 `ranking-aggregator`**로 같은 토픽을 독립 소비한다.

- Redis 쓰기 실패가 DB 집계 오프셋에 영향을 주지 않고(그 반대도), 각자 재처리·랙 관리가 독립적이다.
- 비용: 같은 이벤트를 두 번 역직렬화. 처리량이 문제되면 Nice-to-Have의 배치 정제로 최적화.

### 2.3 N+1 회피 — Facade 배치 조립

랭킹 페이지의 `productId`를 모아 **3번의 배치 조회**로 조립한다.

- `ProductService.findActiveByIds` / `ProductMetricsService.getLikeCounts` / `BrandService.findByIds`.
- 페이지 크기(기본 20)만큼의 개별 조회 대신 상수 쿼리. ZSET엔 있으나 비활성/삭제된 상품은 `findActiveByIds` 결과에서 빠져 자연스럽게 제외.

### 2.4 상품 상세 rank는 캐시 밖

상세의 사용자 무관 데이터는 `ProductReadCache`에 캐시되지만, `rank`는 매초 바뀌는 실시간 값이라 **캐시하지 않는다**. `liked`와 동일하게 Facade가 캐시 결과에 실시간으로 덧붙인다(`toInfo(liked, rank)`).

### 2.5 일자 버킷 = 이벤트 발생시각

컨슘 시각이 아니라 `occurredAt`(KST 환산)으로 날짜 키를 정한다. 컨슈머 랙이나 자정 근처 이벤트도 올바른 날짜에 반영된다. 파싱 실패/누락 시 현재 KST 일자로 폴백(유실 방지).

---

## 3. 남은 일 (TODO)

- [x] 랭킹 **단위 테스트**: `RankingScorePolicy`(가중치·log·delta 부호), `RankingKey`(포맷·KST 환산·폴백), `RankingAggregator`(배치 coalescing·이벤트 분기·**멱등 3종**, Mockito).
- [x] 랭킹 **통합 테스트**: `RankingRedisRepository` 쓰기/읽기(실제 Redis Testcontainer) — `ZINCRBY`+TTL, `ZREVRANGE`/`ZCARD`/`ZREVRANK`.
- [x] Docker로 **E2E** 확인(이벤트 발행 → 컨슘 → ZSET → API 노출) — §4.
- [ ] (Nice-to-Have) 시간 단위 랭킹, 콜드 스타트 워밍업.
- [ ] (선택) 가중치 `application.yml` 외부화.
- [ ] (후속·랭킹 밖) 하이브리드 Outbox의 중복 발행 자체를 줄일지 검토 — 즉시 발행 경로가 행을 `SELECT … FOR UPDATE`로 잡거나 `markSent`를 조건부 UPDATE(`WHERE status='PENDING'`)로 선점하면 경합 창이 좁아진다. 지금은 소비자 멱등으로 흡수되므로 급하지 않고, 발행측 변경은 week7 자산 전체에 영향이라 별도 판단이 필요하다.

---

## 4. E2E 검증 기록 (2026-07-17)

로컬 실기동(commerce-api:8080 / commerce-streamer:8090 / pg-simulator:8082 + `docker/infra-compose.yml`)으로 전 구간을 실측했다.

**시나리오**: 상품 3개에 서로 다른 신호를 넣고 랭킹 순서·스코어를 손계산과 대조.

| 상품 | 신호 | 기대 스코어 | 실측 |
| --- | --- | --- | --- |
| P1 (30,000원) | 조회3 + 좋아요 + 결제완료 주문 1건 | `0.3 + 0.2 + 0.6·log10(30001)` = 3.1862814 | **3.1862814385766738** ✅ |
| P2 (20,000원) | 조회5 | 0.5 | **0.5** ✅ |
| P3 (10,000원) | 조회1 + 좋아요 + 취소 | 0.1 | **−0.1** ❌ → §2.1 중복 발행 |

- 주문 결제는 pg-simulator 콜백으로 `PAID` 확정까지 실제로 태웠고, P1 스코어가 손계산과 소수점까지 일치해 **`unitPrice` 전달 + log 정규화**가 실동작함을 확인했다.
- 랭킹 API가 순위·브랜드명·좋아요수를 배치 조회로 조립(§2.3), 상품 상세 `rank`도 노출 확인.
- 키 `ranking:all:20260717`, `TTL 172751s`(≈2일) — 스펙대로.
- P3의 어긋남이 §2.1의 멱등 개정으로 이어졌다. **E2E가 아니었으면 단위·통합 테스트만으론 절대 못 잡았을 결함**이다 — 두 테스트 모두 "이벤트는 한 번만 온다"를 전제로 짜여 있었기 때문이다.
