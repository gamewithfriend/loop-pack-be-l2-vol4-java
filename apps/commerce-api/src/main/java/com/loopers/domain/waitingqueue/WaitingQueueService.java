package com.loopers.domain.waitingqueue;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

/**
 * 대기열 도메인 규칙: 멱등 진입, 순번/ETA 산정, (Step 2) 토큰 발급 배치·검증·소모.
 *
 * <p>상태는 Redis에만 있으므로 RDB 트랜잭션은 없다. 순서·활성 카운트·토큰 검증은 마스터 노드 읽기를
 * 쓰는 어댑터(RedisEntryTokenRepository/RedisWaitingQueueRepository)에 위임한다(04 §0 정합성 규칙).
 */
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
}
