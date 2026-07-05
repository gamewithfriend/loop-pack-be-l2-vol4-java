package com.loopers.interfaces.api.admin;

import com.loopers.domain.waitingqueue.WaitingQueueStatusView;

public class AdminWaitingQueueV1Dto {

    public record StatusResponse(
        long queueSize,
        long activeCount,
        int maxActive,
        int nextBatchSize,
        double throughputPerSecond,
        long estimatedTailWaitSeconds
    ) {
        public static StatusResponse from(WaitingQueueStatusView view) {
            return new StatusResponse(
                view.queueSize(),
                view.activeCount(),
                view.maxActive(),
                view.nextBatchSize(),
                view.throughputPerSecond(),
                view.estimatedTailWaitSeconds()
            );
        }
    }
}
