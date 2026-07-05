package com.loopers.application.waitingqueue;

import com.loopers.domain.waitingqueue.QueueSnapshot;
import com.loopers.domain.waitingqueue.WaitingQueueService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

/**
 * 대기열 대고객 유스케이스 조립. (Step 3에서 순번 조회 앞단에 RankCache를 붙인다.)
 */
@RequiredArgsConstructor
@Component
public class WaitingQueueFacade {

    private final WaitingQueueService service;

    public QueueSnapshot enter(Long userId) {
        return service.enter(userId);
    }

    public QueueSnapshot getRank(Long userId) {
        return service.resolve(userId);
    }
}
