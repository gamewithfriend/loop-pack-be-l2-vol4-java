# 04. Redis 데이터 모델 — 대기열 (Virtual Waiting Room)

[`01-requirements.md`](./01-requirements.md) §5·§9와 [`02-sequence-diagrams.md`](./02-sequence-diagrams.md)의 Redis 조작을 키 설계 수준으로 확정한다. 이 주차는 RDB 테이블을 추가하지 않는다(week7 ERD 불변). 모든 상태는 Redis에 둔다.

## 0. 전제 — 기존 Redis 인프라 (검증됨)

`modules/redis/config/redis/RedisConfig.java` 기준:

- **Master-Replica 정적 구성** (`RedisStaticMasterReplicaConfiguration`).
- `defaultRedisTemplate` (`@Primary`) → `ReadFrom.REPLICA_PREFERRED` — **읽기가 복제본으로 가서 복제 지연(stale) 가능**.
- `masterRedisTemplate` (`@Qualifier("redisTemplateMaster")`) → `ReadFrom.MASTER` — 항상 마스터 읽기.
- 직렬화: 키/값 모두 `StringRedisSerializer` (`RedisTemplate<String,String>`).

> **핵심 설계 규칙 (정합성)**: 대기열의 **순서·활성 카운트·토큰 검증**은 복제 지연을 허용하면 안 된다(중복 발급·상한 초과·유효 토큰 오거부 위험). 따라서 **쓰기 + 정합성이 중요한 읽기(스케줄러의 ZCARD/ZPOPMIN, 토큰 검증 GET, 진입 시 중복 체크)는 `masterRedisTemplate`을 사용한다.** 오직 **순번 조회(rank)** 만 복제 지연을 감수하고 `defaultRedisTemplate`(replica-preferred)로 읽어도 된다 — 이미 1~2초 캐싱(D5)으로 근사값이 허용되기 때문. 아래 각 키에 사용 템플릿을 명시한다.

---

## 1. 키 목록

| 키 | 자료구조 | 사용 템플릿 | TTL | 용도 |
| --- | --- | --- | --- | --- |
| `waiting:queue` | ZSET | **master** | 없음(영속) | 대기 순서. member=userId, score=seq |
| `waiting:seq` | String(INCR) | **master** | 없음 | 진입 순서 단조 증가 시퀀스(FIFO 타이브레이커) |
| `pass:{token}` | String | **master** | 60s (D1) | 토큰→userId. 가드 검증·자동 만료 |
| `user-pass:{userId}` | String | **master** | 60s | userId→token 역참조(중복 발급 방지·재조회) |
| `active:users` | ZSET | **master** | 없음(원소별 score=만료시각) | 활성 인원 정확 카운트·back-pressure(D6) |
| `rank:cache:{userId}` | String(JSON) | default(replica 허용) | 1~2s (D5) | 순번 조회 결과 캐시 |

> 키 네임스페이스에 `waiting:` / `pass:` / `active:` 접두어를 두어 향후 이벤트·상품별 큐 분리 시 `waiting:{eventId}:queue` 형태로 확장 가능하게 한다(01 Scope: 단일 글로벌 큐로 시작).

---

## 2. 키별 상세

### 2.1 `waiting:queue` — 대기 순서 (ZSET)

- **member** = userId, **score** = `waiting:seq`에서 받은 단조 증가 정수.
- 진입: `ZADD waiting:queue NX <seq> <userId>` — NX로 이미 있는 유저의 score를 덮어쓰지 않음(멱등, NFR-2).
- 순번: `ZRANK waiting:queue <userId>` → 0-based 앞선 인원 수(aheadCount). rank(1-based) = ZRANK + 1.
- 배치 pop: `ZPOPMIN waiting:queue <k>` → score 최솟값(가장 먼저 진입) k명(FIFO, NFR-1).
- 총 대기: `ZCARD waiting:queue`.

**score를 시각(ms) 대신 시퀀스로 두는 이유**: 같은 ms에 다수 진입 시 score 충돌 → ZSET은 동점 시 member(문자열) 사전순으로 정렬해 진입 순서가 뒤바뀐다. `INCR`로 전역 유일·단조 seq를 부여하면 결정적 FIFO가 보장된다(NFR-1). `INCR`은 마스터 원자 연산.

### 2.2 `waiting:seq` — 진입 시퀀스 (String)

- `INCR waiting:seq` → 다음 seq. 원자적, 경합 안전.
- 오버플로우: Long 범위라 실질적 무한. 이벤트 종료 후 큐 비면 리셋(선택).

### 2.3 `pass:{token}` — 입장 토큰 (String + TTL)

- 값 = ownerUserId. `SET pass:{token} <userId> EX 60`.
- 토큰은 불투명 문자열(UUID v4 또는 난수 base64). 추측 불가.
- 가드 검증(§02 S2-2): `GET pass:{token}` →
  - nil → 만료/미존재 → 403.
  - ownerUserId ≠ X-USER-ID → 불일치 → 403(토큰 도용 차단).
  - 일치 + 존재 → 통과.
- 소모(주문 성공 확정, D3): `DEL pass:{token}`.
- **TTL이 곧 만료 정책** — 미사용 토큰은 Redis가 자동 회수(FR-4). 별도 만료 스케줄러 불필요.

### 2.4 `user-pass:{userId}` — 역참조 (String + TTL)

- 값 = token. `SET user-pass:{userId} <token> EX 60`.
- 용도: ① 한 유저가 이미 활성인지 O(1) 확인(스케줄러 재발급·enter 시 READY 판정 보조), ② 소모 시 token을 몰라도 userId로 찾아 `pass:{token}`까지 정리, ③ 클라이언트 토큰 유실 시 재조회.
- 소모 시 함께 `DEL`.

### 2.5 `active:users` — 활성 세트 (ZSET, D6 핵심)

- **member** = userId, **score** = 만료시각(epoch ms) = 발급시각 + TTL.
- 발급: `ZADD active:users <now+60000> <userId>`.
- 소모(주문 성공): `ZREM active:users <userId>`.
- **정확 카운트 (스케줄러 매 주기)**:
  1. `ZREMRANGEBYSCORE active:users 0 <now>` — 만료 원소 일괄 제거.
  2. `ZCARD active:users` → 현재 활성 인원 `activeCount`.
  3. `batchSize = max(0, maxActive − activeCount)` (P-5, D2).
- **왜 필요한가**: `pass:{token}` 개별 TTL만으론 "지금 활성 몇 명"을 O(1)로 셀 수 없어 스케줄러가 리필량을 못 정한다. `active:users`가 활성 카운트의 단일 진실원(SoT). TTL 자동삭제(`pass`)와 명시적 청소(`active` ZREMRANGEBYSCORE)를 병행하는 이유다.

> **정합성 주의**: `pass:{token}`(TTL 자동)과 `active:users`(명시 청소)는 만료 타이밍이 미세하게 어긋날 수 있다(pass는 60s 정확, active는 다음 스케줄러 주기에 청소). 이는 **back-pressure를 보수적으로** 만들 뿐(활성을 실제보다 잠깐 많게 셈 → 덜 발급) 상한 초과 방향의 위험은 없어 안전하다.

### 2.6 `rank:cache:{userId}` — 순번 캐시 (String, D5)

- 값 = `{status, rank, aheadCount, eta, pollAfterSeconds}` JSON. `SET rank:cache:{userId} <json> EX 2`.
- 조회(FR-6): 캐시 히트면 Redis ZSET 조회 생략(폴링 폭주 흡수, NFR-7).
- **replica 읽기 허용**: rank는 근사값이어도 UX 무해 → `defaultRedisTemplate`. 단 캐시 자체 write는 아무 노드나 무방(짧은 TTL).

---

## 3. 원자성 & 경쟁 조건

### 3.1 배치 발급의 원자성 (§02 S2-1 note)

`ZPOPMIN`으로 큐에서 뺀 뒤 토큰 3키(`pass`·`user-pass`·`active`) 저장 중 장애 시, 유저가 큐에서도 빠지고 토큰도 없는 **유실**이 생길 수 있다.

**채택: Lua 스크립트로 pop+발급 원자화.** 스케줄러가 `EVAL`로 아래를 한 번에 실행한다.

```lua
-- KEYS: waiting:queue, active:users
-- ARGV: k(뽑을 수), now, ttlMs, maxActive, [token_1..token_k 선생성]
-- 1) 만료 청소 + 활성 카운트
redis.call('ZREMRANGEBYSCORE', KEYS[2], 0, ARGV[2])
local active = redis.call('ZCARD', KEYS[2])
local room = tonumber(ARGV[4]) - active
if room <= 0 then return {} end
local n = math.min(tonumber(ARGV[1]), room)
-- 2) 앞에서 n명 pop + 활성 등록 (token은 애플리케이션이 미리 생성해 ARGV로 전달)
local popped = redis.call('ZPOPMIN', KEYS[1], n)  -- [member, score, ...]
local issued = {}
local expireAt = tonumber(ARGV[2]) + tonumber(ARGV[3])
local ai = 5  -- ARGV[5]부터 토큰
for i = 1, #popped, 2 do
  local uid = popped[i]
  redis.call('ZADD', KEYS[2], expireAt, uid)
  issued[#issued+1] = uid
  issued[#issued+1] = ARGV[ai]; ai = ai + 1
end
return issued  -- [userId, token, ...]
```

이후 애플리케이션이 반환된 `(userId, token)` 쌍마다 `SET pass:{token}`·`SET user-pass:{userId}`를 TTL과 함께 기록한다. **pop+active 등록이 원자적**이므로 상한 초과·중복 pop이 없다. `pass` 키 저장이 뒤따르다 실패해도 `active`에 등록돼 있어 슬롯은 TTL로 회수되고, 그 유저는 다음 재진입으로 복구(공정성 유지).

> 단순화 대안: pop과 발급을 애플리케이션 트랜잭션 없이 순차 실행하고, 저장 실패 시 해당 userId를 `ZADD`로 큐에 되돌리는 보상. Lua보다 구현은 쉬우나 상한 초과 경쟁(다중 인스턴스)에 약하다 — **ShedLock으로 스케줄러가 단일 실행(NFR-5)이므로 실은 다중 인스턴스 경쟁은 없다.** 그럼에도 Lua를 기본으로 두는 이유는 "부분 실패 시 상태 정합"을 한 번에 보장하기 위해서다.

### 3.2 멱등 진입 경쟁 (FR-1)

동일 유저가 짧은 간격 두 번 enter → 둘 다 `ZADD NX`. NX 덕분에 두 번째는 no-op, score 유지(순번 안 밀림). 서로 다른 유저 동시 진입은 각자 `INCR`로 유일 seq를 받아 충돌 없음.

### 3.3 토큰 검증-소모 경쟁

같은 토큰으로 주문 API 중복 호출(더블 클릭) → 가드는 `GET`으로 통과시키지만, 주문 로직은 기존 멱등/재고 제어(week6~7)가 판정. 성공 확정은 한 번만 일어나고 그때 `DEL`. 이미 DEL된 뒤 재요청은 `GET` nil → 403(정상 차단).

---

## 4. 장애 시 동작 (D7, fail-closed)

| 상황 | 동작 |
| --- | --- |
| enter 중 Redis 불가 | 503, 진입 실패(재시도 안내) |
| rank 조회 중 Redis 불가 | 503 또는 마지막 캐시(있으면). 신규 계산 불가 |
| 가드 검증 중 Redis 불가 | **주문 진입 차단(503)** — fail-open 금지. 대기열이 백엔드 보호막이므로 |
| 스케줄러 발급 중 Redis 불가 | 이번 주기 스킵, 로그·메트릭. 다음 주기 재시도 |

---

## 5. 관측 (D8)

`GET /api/v1/admin/waiting-queue/status`가 노출하고 Prometheus로도 내보낼 지표:

- `waiting_queue_size` = `ZCARD waiting:queue` (대기 인원)
- `waiting_active_count` = `ZCARD active:users`(청소 후) (활성 인원)
- `waiting_max_active`, `waiting_batch_size`, `waiting_throughput_per_sec` (정책 파생값)
- `waiting_tokens_issued_total`, `waiting_tokens_expired_total`, `waiting_tokens_consumed_total` (카운터)
- `waiting_eta_seconds`(대기열 꼬리 기준 예상 대기) — 튜닝 관측용

> week7의 Grafana/Prometheus 스크레이프 배선을 재사용한다(신규 인프라 없음).
