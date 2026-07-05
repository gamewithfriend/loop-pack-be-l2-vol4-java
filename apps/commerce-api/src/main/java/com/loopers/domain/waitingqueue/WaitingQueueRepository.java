package com.loopers.domain.waitingqueue;

import java.util.List;

/**
 * 대기 순서 저장소(포트). 구현은 Redis Sorted Set(waiting:queue) — docs/week8/04-redis-model.md §2.1.
 */
public interface WaitingQueueRepository {

    /** 대기열 진입(멱등). 신규 등록이면 true, 이미 대기 중이면 false(순번 유지). */
    boolean enqueueIfAbsent(Long userId);

    /** 0-based 순번(앞선 인원 수). 대기열에 없으면 null. */
    Long rank(Long userId);

    boolean isQueued(Long userId);

    /** 총 대기 인원. */
    long size();

    /** 앞에서 k명 pop(FIFO). 발급 배치용(Step 2). */
    List<Long> popFront(int k);

    /** 발급 실패 등으로 대기열 뒤로 되돌림(새 순서 부여). */
    void requeue(Long userId);
}
