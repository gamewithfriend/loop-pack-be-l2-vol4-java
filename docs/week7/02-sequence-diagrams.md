# 02. 시퀀스 다이어그램 — 이벤트 기반 아키텍처 (Event-Driven)

[`01-requirements.md`](./01-requirements.md) §6의 Step1~3 흐름을 레이어별 참여자 기준으로 시각화한다. 표기 규칙은 [`../week2/02-sequence-diagrams.md`](../week2/02-sequence-diagrams.md) §0을 따른다(레이어/화살표/생략/공통 에러). 이 문서의 결정 근거는 [`01-requirements.md`](./01-requirements.md) §9 결정사항 표를 따른다.

## 0. 참여자 (기존 레이어 + week7 신규)

### commerce-api (Producer)

| 약칭 | 클래스/컴포넌트 | 레이어 | 책임 |
| --- | --- | --- | --- |
| `OFac` | `OrderFacade` | Application | 주문 유스케이스 조립 |
| `OSvc` | `OrderService` | Domain Service | 주문 생성·상태 전이 |
| `PFac` | `PaymentFacade` | Application | 결제 시작/콜백/reconcile 조립 |
| `PCfm` | `PaymentConfirmer` | Application | 결제 확정 단위(콜백·reconcile 공유) |
| `LSvc` | `LikeService` | Domain Service | 좋아요 상태 전이 |
| `CFac` | `CouponFacade` | Application | 쿠폰 대고객 유스케이스 |
| `Pub` | `ApplicationEventPublisher` | Spring | 인-프로세스 도메인 이벤트 발행 |
| `Appender` | `OutboxAppender` | Domain/Infra | 도메인 트랜잭션 안에서 `outbox` INSERT |
| `OBox` | `outbox` 테이블 | DB | 발행 대기 메시지(PENDING/SENT/FAILED) |
| `Relay` | `OutboxRelayScheduler` | Infra (@Scheduled+ShedLock) | PENDING 폴링 → Kafka 전송 → SENT 마킹 |
| `KT` | `KafkaTemplate` | Infra | Kafka 발행(acks=all, idempotence=true) |

### Kafka

| 토픽 | 키 | eventType 예 |
| --- | --- | --- |
| `catalog-events` | productId | LIKE_CHANGED, PRODUCT_VIEWED |
| `order-events` | orderId | ORDER_PAID |
| `coupon-issue-requests` | couponId | COUPON_ISSUE_REQUESTED |

### commerce-streamer (Consumer)

| 약칭 | 클래스/컴포넌트 | 책임 |
| --- | --- | --- |
| `MCon` | `MetricsConsumer` (group=metrics-aggregator) | catalog/order 이벤트 소비 → 집계 |
| `CCon` | `CouponIssueConsumer` (group=coupon-issuer) | 발급 요청 소비 → 발급 처리 |
| `Idem` | `EventHandledChecker` | `event_handled(event_id)` 멱등 확인/기록 |
| `EH` | `event_handled` 테이블 | 처리 완료 표식(event_id PK) |
| `PM` | `product_metrics` 테이블 | 좋아요/판매량/조회 수 upsert |
| `CpnW` | `coupon` 테이블 | 원자 UPDATE(issued_count) |
| `UCW` | `user_coupon` 테이블 | 발급분 INSERT |
| `CIR` | `coupon_issue_request` 테이블 | 발급 결과(PENDING/ISSUED/SOLD_OUT/REJECTED) |

> **공통 envelope**: 모든 메시지는 `{ eventId, eventType, aggregateId, occurredAt, version, payload }`를 공유한다. `eventId`는 outbox PK(전역 고유) = 소비자 멱등 키.

---

## Step 1. ApplicationEvent 경계 분리

### S1-1. 결제 확정 시 부가 로직 분리 (AFTER_COMMIT)

주요 로직(결제 확정 → 주문 PAID)과 부가 로직(판매량 전파용 이벤트, 알림)을 분리한다. 부가 리스너 실패가 결제 트랜잭션을 롤백시키지 않는다.

```mermaid
sequenceDiagram
    participant PCfm as PaymentConfirmer
    participant OSvc as OrderService
    participant Pub as ApplicationEventPublisher
    participant L1 as OrderPaidKafkaListener<br/>(AFTER_COMMIT)
    participant L2 as NotificationListener<br/>(AFTER_COMMIT)
    participant Appender as OutboxAppender

    rect rgb(235,245,255)
    note over PCfm,OSvc: 주요 트랜잭션 (TX)
    PCfm->>OSvc: markPaid(orderId)
    OSvc->>OSvc: 상태 PENDING→PAID
    PCfm->>Pub: publishEvent(OrderPaidEvent)
    note right of Pub: 발행은 TX 안, 리스너 실행은 커밋 후
    end

    rect rgb(235,255,235)
    note over L1,L2: 커밋 성공 후에만 실행
    Pub-->>L1: onOrderPaid (AFTER_COMMIT)
    L1->>Appender: append(order-events, ORDER_PAID)
    Pub-->>L2: onOrderPaid (AFTER_COMMIT)
    L2->>L2: 알림 트리거 (mock/log)
    end

    note over L1,L2: 리스너 예외는 격리(로그) — 주요 흐름에 전파 X
```

> **판단 기준**: `OrderPaidEvent`는 (a) 결제 확정과 원자적일 필요 없음 + (b) 결과에 영향 없는 후속 + (c) 타 시스템(streamer 판매량 집계)이 필요 → **이벤트 분리 + Kafka 전파(outbox)** 대상. 알림은 (c)가 없으니 ApplicationEvent까지만.

### S1-2. 좋아요 상태 전이 (결과적 일관성, 기존 일반화)

```mermaid
sequenceDiagram
    actor U as Client(User)
    participant LSvc as LikeService
    participant Pub as ApplicationEventPublisher
    participant LL as LikeEventListener<br/>(AFTER_COMMIT)
    participant Appender as OutboxAppender

    U->>LSvc: like(userId, productId)
    rect rgb(235,245,255)
    note over LSvc: 주요 TX — product_like INSERT/activate
    LSvc->>LSvc: 실제 상태 전이 발생 시에만
    LSvc->>Pub: publishEvent(LikeChangedEvent +1)
    end
    Pub-->>LL: onLikeChanged (AFTER_COMMIT)
    LL->>Appender: append(catalog-events, LIKE_CHANGED, delta=+1)
    note over LL: 즉시 likes_count 갱신 X → 비동기 집계로 위임
```

---

## Step 2. Kafka 이벤트 파이프라인

### S2-1. Outbox 발행 (도메인 변경과 단일 트랜잭션)

핵심 불변식: **도메인 변경 INSERT/UPDATE 와 outbox INSERT 는 같은 트랜잭션**. 둘 다 커밋되거나 둘 다 롤백 → 메시지 유실(at-most-once) 제거.

```mermaid
sequenceDiagram
    participant L as 리스너/서비스
    participant Appender as OutboxAppender
    participant OBox as outbox (DB)

    rect rgb(235,245,255)
    note over L,OBox: 도메인 변경과 동일 TX
    L->>Appender: append(topic, eventType, key, payload)
    Appender->>OBox: INSERT (status=PENDING, eventId=PK)
    note right of OBox: 도메인 row + outbox row가<br/>원자적으로 커밋
    end
```

### S2-2. Outbox 릴레이 → Kafka (폴링 스케줄러)

```mermaid
sequenceDiagram
    participant Sch as OutboxRelayScheduler<br/>(@Scheduled, ShedLock)
    participant OBox as outbox (DB)
    participant KT as KafkaTemplate
    participant K as Kafka

    loop 주기 폴링 (예: 1s)
        Sch->>OBox: SELECT * WHERE status=PENDING ORDER BY id LIMIT N
        OBox-->>Sch: PENDING 메시지 배치
        loop 각 메시지
            Sch->>KT: send(topic, key, envelope)
            KT->>K: produce (acks=all, idempotence=true)
            alt 전송 성공
                K-->>KT: ack
                Sch->>OBox: UPDATE status=SENT, sent_at=now
            else 전송 실패
                K-->>KT: error
                Sch->>OBox: PENDING 유지 (다음 주기 재시도 = At Least Once)
                note right of Sch: 반복 실패 시 FAILED 마킹 + 운영 알림
            end
        end
    end
    note over Sch: ShedLock으로 다중 인스턴스 중복 폴링 방지
```

### S2-3. 집계 Consumer — product_metrics upsert (멱등 + 최신성)

```mermaid
sequenceDiagram
    participant K as Kafka<br/>(catalog/order-events)
    participant MCon as MetricsConsumer<br/>(manual ack)
    participant Idem as EventHandledChecker
    participant EH as event_handled (DB)
    participant PM as product_metrics (DB)

    K-->>MCon: poll batch(records)
    loop 각 record
        MCon->>Idem: seen(eventId)?
        Idem->>EH: SELECT 1 WHERE event_id=?
        alt 이미 처리됨
            EH-->>Idem: 존재
            Idem-->>MCon: skip
        else 신규
            EH-->>Idem: 없음
            MCon->>PM: upsert (eventType별 집계)
            note right of PM: LIKE_CHANGED → like_count += delta<br/>ORDER_PAID → sales_count += qty<br/>PRODUCT_VIEWED → view_count += 1<br/>WHERE version > 기존 version (stale 무시)
            MCon->>EH: INSERT event_id (멱등 표식)
        end
    end
    MCon->>K: manual ack(commit offset)
    note over MCon: 처리 성공 후에만 ack — 실패 시 재처리(At Least Once)
```

> **최신성**: upsert 시 envelope `version`(또는 occurredAt)을 비교해 더 오래된 이벤트가 최신 집계를 덮어쓰지 않게 한다. 좋아요 delta는 교환법칙이 성립하므로 합산은 순서 비의존.

### S2-4. reconcile 안전망 (이벤트 유실 보정)

```mermaid
sequenceDiagram
    participant Sch as MetricsReconciler<br/>(@Scheduled)
    participant SRC as 원천 테이블<br/>(product_like, order_item)
    participant PM as product_metrics

    loop 주기 (예: 10분)
        Sch->>SRC: 집계 재계산 (COUNT/SUM)
        Sch->>PM: 카운터 보정 UPDATE
    end
    note over Sch: 이벤트 유실/순서 이상에도 결국 정합(eventual)
```

---

## Step 3. Kafka 기반 선착순 쿠폰 발급

### S3-1. 발급 요청 (API는 발행만, 202 Accepted)

```mermaid
sequenceDiagram
    actor U as Client(User)
    participant CCtrl as CouponV1Controller
    participant CFac as CouponFacade
    participant CIR as coupon_issue_request (DB)
    participant Appender as OutboxAppender
    participant OBox as outbox (DB)

    U->>CCtrl: POST /api/v1/coupons/{couponId}/issue-requests
    CCtrl->>CFac: requestIssue(userId, couponId)
    rect rgb(235,245,255)
    note over CFac,OBox: 단일 TX
    CFac->>CFac: 사전검증(쿠폰 존재/기간) — 실패 시 4xx
    CFac->>CIR: INSERT (status=PENDING, requestId)<br/>UNIQUE(user_id, coupon_id)
    alt 중복 요청 (이미 존재)
        CIR-->>CFac: unique 위반
        CFac-->>CCtrl: 기존 requestId 반환(멱등)
    else 신규 접수
        CFac->>Appender: append(coupon-issue-requests, key=couponId)
        Appender->>OBox: INSERT (PENDING)
    end
    end
    CCtrl-->>U: 202 Accepted { requestId }
    note over U: 실제 발급은 비동기 — 결과는 S3-3로 조회
```

### S3-2. 발급 처리 Consumer (원자 UPDATE + 멱등)

선착순 불변식 `issued_count <= total_quantity`를 어떤 동시성에서도 위반하지 않는다. key=couponId로 같은 쿠폰 요청이 같은 파티션에 직렬화된다.

```mermaid
sequenceDiagram
    participant K as Kafka<br/>(coupon-issue-requests)
    participant CCon as CouponIssueConsumer<br/>(manual ack)
    participant Idem as EventHandledChecker
    participant Cpn as coupon (DB)
    participant UCW as user_coupon (DB)
    participant CIR as coupon_issue_request (DB)

    K-->>CCon: poll(record: requestId, couponId, userId)
    CCon->>Idem: seen(requestId)?
    alt 이미 처리됨
        Idem-->>CCon: skip → ack
    else 신규
        rect rgb(255,245,235)
        note over CCon,CIR: 발급 TX
        CCon->>Cpn: UPDATE coupon SET issued_count=issued_count+1<br/>WHERE id=? AND issued_count < total_quantity
        alt 영향 행 = 1 (수량 확보)
            CCon->>UCW: INSERT user_coupon (발급분)
            CCon->>CIR: UPDATE status=ISSUED
        else 영향 행 = 0 (수량 소진)
            CCon->>CIR: UPDATE status=SOLD_OUT
            note right of CIR: 예외 아님 — 재처리해도 발급 안 됨
        end
        CCon->>Idem: INSERT event_handled(requestId)
        end
    end
    CCon->>K: manual ack
```

> **1인 1매**: `coupon_issue_request`의 `UNIQUE(user_id, coupon_id)`(S3-1)가 요청 단계에서, `event_handled(requestId)`가 소비 단계에서 이중 차단. 자격 미달은 `status=REJECTED`로 기록.

### S3-3. 발급 결과 조회

```mermaid
sequenceDiagram
    actor U as Client(User)
    participant CCtrl as CouponV1Controller
    participant CFac as CouponFacade
    participant CIR as coupon_issue_request (DB)

    U->>CCtrl: GET /api/v1/coupons/issue-requests/{requestId}
    CCtrl->>CFac: getIssueResult(userId, requestId)
    CFac->>CIR: SELECT status WHERE request_id=?
    CIR-->>CFac: PENDING | ISSUED | SOLD_OUT | REJECTED
    CFac-->>CCtrl: 결과
    CCtrl-->>U: 200 { status, (issued면 userCouponId) }
```

---

## 부록. 좋아요 토픽 마이그레이션 (스키마 통일)

기존 `catalog.like-changed.v1` → `catalog-events`(공통 envelope) 전환. 무중단 이전 절차.

```mermaid
sequenceDiagram
    participant API as commerce-api
    participant Old as catalog.like-changed.v1
    participant New as catalog-events
    participant SOld as LikeCountConsumer(구)
    participant SNew as MetricsConsumer(신)

    note over API,SNew: 1단계 — 신규 발행/소비 추가 (병행)
    API->>New: LIKE_CHANGED (신규)
    SNew->>SNew: catalog-events 소비 시작
    note over API,SOld: 2단계 — 구 토픽 발행 중단
    API--xOld: 발행 중단
    SOld->>SOld: 잔여 메시지 소진 후 폐기
    note over SNew: 3단계 — catalog-events 단독 운영
```

> reconcile(S2-4)이 원천에서 카운트를 재보정하므로, 이전 중 짧은 불일치는 결국 수렴한다.
