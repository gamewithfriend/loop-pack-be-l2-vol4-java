package com.loopers.domain.waitingqueue;

import com.loopers.support.error.CoreException;
import com.loopers.support.error.ErrorType;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;

/**
 * 대기열 도메인 규칙: 멱등 진입, 순번/ETA 산정, 토큰 발급 배치·검증·소모.
 *
 * <p>상태는 Redis에만 있으므로 RDB 트랜잭션은 없다. 순서·활성 카운트·토큰 검증은 마스터 노드 읽기를
 * 쓰는 어댑터(RedisEntryTokenRepository/RedisWaitingQueueRepository)에 위임한다(04 §0 정합성 규칙).
 */
@Slf4j
@RequiredArgsConstructor
@Component
public class WaitingQueueService {

    private final WaitingQueueRepository queue;
    private final EntryTokenRepository tokens;
    private final ThroughputPolicy policy;

    /** 대기열 진입(FR-1). 이미 활성이면 READY, 이미/신규 대기면 WAITING(순번 유지, 멱등). */
    public QueueSnapshot enter(Long userId) {
        if (tokens.isActive(userId)) {
            return QueueSnapshot.ready();
        }
        queue.enqueueIfAbsent(userId);
        return currentSnapshot(userId);
    }

    /** 순번/상태 조회(FR-2·FR-6). */
    public QueueSnapshot resolve(Long userId) {
        if (tokens.isActive(userId)) {
            return QueueSnapshot.ready();
        }
        return currentSnapshot(userId);
    }

    private QueueSnapshot currentSnapshot(Long userId) {
        Long rank0 = queue.rank(userId);
        if (rank0 == null) {
            // 진입 직후 스케줄러가 pop해 활성으로 넘어갔을 수 있음 → 활성 재확인
            return tokens.isActive(userId) ? QueueSnapshot.ready() : QueueSnapshot.notInQueue();
        }
        long ahead = rank0;
        long rank = rank0 + 1;
        return QueueSnapshot.waiting(rank, ahead, policy.estimatedWaitSeconds(ahead));
    }

    /**
     * 토큰 발급 배치(FR-3, 스케줄러가 호출). 만료 청소 후 활성 여유분(=batchSize)만큼 앞에서 pop해 발급.
     * 발급 실패한 유저는 큐 뒤로 되돌려 유실을 방지한다(공정성). 발급 인원 수를 반환한다.
     */
    public int issueBatch() {
        long active = tokens.purgeExpiredAndCount();
        int k = policy.batchSize(active);
        if (k <= 0) {
            return 0;
        }
        List<Long> userIds = queue.popFront(k);
        int issued = 0;
        for (Long userId : userIds) {
            String token = newToken();
            try {
                tokens.issue(userId, token, policy.tokenTtlSeconds());
                issued++;
            } catch (RuntimeException e) {
                log.warn("입장 토큰 발급 실패 → 재큐잉 userId={}", userId, e);
                queue.requeue(userId);
            }
        }
        return issued;
    }

    /**
     * 주문 API 진입 가드(FR-5). 토큰 (a)존재 (b)미만료 (c)유저 일치를 검증한다.
     * 실패 시 {@link ErrorType#FORBIDDEN}. (만료는 Redis TTL로 pass 키가 사라져 존재하지 않음으로 나타난다.)
     */
    public void validateToken(Long userId, String token) {
        if (token == null || token.isBlank()) {
            throw new CoreException(ErrorType.FORBIDDEN, "입장 토큰이 필요합니다.");
        }
        Long owner = tokens.findUserIdByToken(token);
        if (owner == null) {
            throw new CoreException(ErrorType.FORBIDDEN, "만료되었거나 유효하지 않은 입장 토큰입니다.");
        }
        if (!owner.equals(userId)) {
            throw new CoreException(ErrorType.FORBIDDEN, "입장 토큰의 소유자가 일치하지 않습니다.");
        }
    }

    /** 토큰 소모(주문 성공 확정 시). 활성 슬롯 회수(P-2, D3). */
    public void consume(Long userId, String token) {
        tokens.consume(userId, token);
    }

    /** 운영 현황(Admin·관측, FR-7). 부수효과 없는 읽기. */
    public WaitingQueueStatusView status() {
        long queueSize = queue.size();
        long active = tokens.activeCountLive();
        return new WaitingQueueStatusView(
            queueSize,
            active,
            policy.maxActive(),
            policy.batchSize(active),
            policy.throughputPerSecond(),
            policy.estimatedWaitSeconds(queueSize)
        );
    }

    private String newToken() {
        return UUID.randomUUID().toString().replace("-", "");
    }
}
