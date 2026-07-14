# 05. 구현 노트 — 실시간 랭킹

구현 시점의 설계 트레이드오프와 파일 맵을 기록한다. 요구사항은 [`01-requirements.md`](./01-requirements.md), 데이터 모델은 [`04-redis-model.md`](./04-redis-model.md) 참조.

> **상태**: Must-Have 구현 완료(커밋 `fe63919`, branch `volume-9`). Nice-to-Have(시간 단위 랭킹·콜드 스타트)는 미포함. 테스트 미작성, Docker 통합/E2E 미실행(양 앱 `compileJava`/`compileTestJava`는 JDK21로 통과).

---

## 1. 파일 맵

### commerce-streamer (적재/쓰기)

| 파일 | 역할 |
| --- | --- |
| `interfaces/consumer/ProductRankingConsumer` | `@KafkaListener` 배치, 그룹 `ranking-aggregator`, 수동 커밋 |
| `application/ranking/RankingAggregator` | 배치 파싱 + `(키, productId)` 델타 coalescing |
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
| `domain/order/OrderPaidEvent`(api) | `Item`에 `unitPrice` 추가(원본 주석 보존) |
| `application/product/ProductFacade`(api) | `RankingRepository` 주입, 상세에 `rank` 조합 |
| `application/product/ProductDetailInfo`(api) | `rank` 필드 추가 |
| `application/product/CachedProductDetail`(api) | `toInfo(liked, rank)`(원본 주석 보존) |
| `interfaces/api/product/ProductV1Dto`(api) | `ProductDetailResponse`에 `rank` 추가 |

---

## 2. 설계 트레이드오프

### 2.1 멱등성 — 왜 `event_handled`를 쓰지 않았나

`product_metrics` 집계(`MetricsAggregator`)는 `event_handled` 테이블로 정확-1회를 보장한다. 랭킹은 그렇게 하지 않았다.

- `ZINCRBY`는 멱등이 아니라, 컨슈머 재전달 시 소폭 이중 가산된다.
- 정확-1회를 하려면 Redis 쓰기와 `event_handled` DB 마킹을 하나의 원자 단위로 묶어야 하는데, **Redis와 DB는 한 트랜잭션이 안 된다**. 순서를 어떻게 잡아도(Redis→DB / DB→Redis) 장애 창(window)에서 이중 가산 또는 유실이 남는다.
- 랭킹은 **근사값 + 2일 TTL 리셋**이므로 소폭 이중 가산을 수용하고 단순성을 택했다.

> **정확성이 필요해지면**: `EventHandledRepository`가 `consumer_group` 파라미터를 받으므로, `ranking-aggregator` 그룹으로 재사용해 "Redis 반영 → 같은 트랜잭션에서 마킹" 순서(at-least-once + 근사 정확성)로 확장할 수 있다. 지금은 과설계로 판단해 보류.

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

- [ ] 랭킹 **단위 테스트**: `RankingScorePolicy`(가중치·log·delta 부호), `RankingKey`(포맷·KST 환산·폴백), `RankingAggregator`(배치 coalescing·이벤트 분기, Mockito).
- [ ] 랭킹 **통합 테스트**: `RankingRedisRepository` 쓰기/읽기(실제 Redis Testcontainer) — `ZINCRBY`+TTL, `ZREVRANGE`/`ZCARD`/`ZREVRANK`.
- [ ] Docker로 **E2E** 확인(이벤트 발행 → 컨슘 → ZSET → API 노출).
- [ ] (Nice-to-Have) 시간 단위 랭킹, 콜드 스타트 워밍업.
- [ ] (선택) 가중치 `application.yml` 외부화.
