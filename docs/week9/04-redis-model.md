# 04. Redis 데이터 모델 — 실시간 랭킹 (ZSET)

[`01-requirements.md`](./01-requirements.md) §3·§4와 [`02-sequence-diagrams.md`](./02-sequence-diagrams.md)의 Redis 조작을 키·스코어 수준으로 확정한다. 이 주차는 RDB 테이블을 추가하지 않는다(week7 ERD 불변). 랭킹 상태는 전부 Redis ZSET에 둔다.

## 0. 전제 — 기존 Redis 인프라

`modules/redis/config/redis/RedisConfig.java` 기준:

- **Master-Replica 정적 구성** (`RedisStaticMasterReplicaConfiguration`).
- `defaultRedisTemplate`(`@Primary`) → `ReadFrom.REPLICA_PREFERRED`. **쓰기는 Lettuce가 자동으로 master로 라우팅**하고, 읽기만 복제본 우선.
- 직렬화: 키/값 모두 `StringRedisSerializer`(`RedisTemplate<String,String>`).

> **템플릿 선택**: 랭킹은 근사값을 허용하므로(NFR-1) 쓰기·읽기 모두 `defaultRedisTemplate`을 쓴다. `ZINCRBY`는 master로 라우팅되고, `ZREVRANGE`/`ZREVRANK`는 복제본 우선(소폭 stale 허용). 대기열(week8)처럼 정합성이 결정적인 경우와 달리, 랭킹은 replica 지연을 감수한다.

---

## 1. 키 설계

| 키 | 자료구조 | member | score | TTL | 용도 |
| --- | --- | --- | --- | --- | --- |
| `ranking:all:{yyyyMMdd}` | ZSET | `productId`(문자열) | 누적 가중 점수(double) | **2 Day** | 일간 전체 상품 랭킹 |

- **일자**: `yyyyMMdd`(KST). 이벤트 `occurredAt`을 KST로 환산한 날짜(`RankingKey.dateOf`).
- **네임스페이스**: `ranking:all:` 접두어. Nice-to-Have(시간 단위·카테고리별)는 `ranking:all:{yyyyMMddHH}`, `ranking:{category}:{yyyyMMdd}`로 확장 가능.
- **앱 경계 계약**: 쓰기(streamer)·읽기(api)가 코드를 공유하지 않으므로 **키 포맷 문자열이 유일한 계약**이다. 양쪽 `RankingKey`가 동일 포맷(`ranking:all:` + `BASIC_ISO_DATE`)을 생성해야 한다.

### 1.1 TTL 정책

- ZSET에 쓸 때마다 `EXPIRE key 2d`로 갱신한다(일간 키라 그 날 마지막 쓰기 기준 2일 뒤 만료).
- 효과: **오늘 + 어제** 두 개의 일간 랭킹이 항상 조회 가능. 과거 데이터는 자동 회수(NFR-4).

---

## 2. 연산

### 2.1 적재 (commerce-streamer)

배치 내 상품별 합산 델타를 파이프라인으로 반영한다.

```
PIPELINE
  ZINCRBY ranking:all:20260714 <scoreDelta> <productId>   // 상품마다 1회(coalesced)
  ...
  EXPIRE  ranking:all:20260714 172800                     // 키마다 1회 (2d)
EXEC
```

- `ZINCRBY`로 누적(원자 가산). 좋아요 취소는 음수 델타로 감점.
- 파이프라인으로 배치 왕복(RTT)을 1회로 압축.

### 2.2 조회 (commerce-api)

| API 동작 | Redis 연산 | 비고 |
| --- | --- | --- |
| 페이지 조회 | `ZREVRANGE key (page-1)*size … WITHSCORES` | 내림차순 상위. rank = offset + index + 1 |
| 전체 건수 | `ZCARD key` | 페이지네이션 total |
| 상품 순위 | `ZREVRANK key member` | 0-based → +1 (1-based). 없으면 null |

- `ZREVRANGE`는 순서(내림차순)를 보존하므로 반환 순서대로 rank를 매긴다.
- `page`는 1-based(API 계약). 0-based offset = `(page-1) × size`.

---

## 3. 스코어 계산식 (`RankingScorePolicy`)

최종 가산치 = **Weight × Score**.

```
조회   : +VIEW_WEIGHT               = +0.1
좋아요 : +LIKE_WEIGHT × delta       = ±0.2         (delta = ±1)
주문   : +ORDER_WEIGHT × log10(1 + unitPrice×quantity)
        = +0.6 × log10(1 + 매출)                    (매출 ≤ 0 이면 0)
```

**스케일 감각 (예시)**

| 신호 | 입력 | 가산치 |
| --- | --- | --- |
| 조회 1회 | — | +0.1 |
| 좋아요 +1 | delta=+1 | +0.2 |
| 좋아요 취소 | delta=-1 | -0.2 |
| 주문 (10,000원 × 2) | 매출 20,000 | +0.6 × log10(20,001) ≈ **+2.58** |
| 주문 (1,000,000원 × 1) | 매출 1,000,000 | +0.6 × log10(1,000,001) ≈ **+3.60** |

> log 정규화 덕에 매출이 100배(2만→100만) 늘어도 가산치는 ~1.4배만 증가한다 → 고가 1건이 조회·좋아요 신호를 삼키지 않는다(D1).

---

## 4. 주문 매출을 위한 이벤트 계약 변경

랭킹 주문 스코어(`price×amount`)를 위해 `ORDER_PAID` payload에 **`unitPrice`** 를 추가했다.

```jsonc
// order-events / ORDER_PAID payload
{
  "orderId": 100,
  "items": [
    { "productId": 30, "quantity": 2, "unitPrice": 10000 }  // unitPrice = week9 추가
  ]
}
```

- **하위 호환**: `product_metrics` 집계 컨슈머는 payload를 `JsonNode`로 읽어 `productId`/`quantity`만 사용하므로 `unitPrice` 추가에 무영향. 구(舊) 이벤트에 `unitPrice`가 없으면 랭킹은 0으로 처리(가산 없음).
- 원본 `OrderPaidEvent.Item(productId, quantity)` 계약은 주석으로 보존.
