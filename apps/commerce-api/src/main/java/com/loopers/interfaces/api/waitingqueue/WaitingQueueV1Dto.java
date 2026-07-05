package com.loopers.interfaces.api.waitingqueue;

import com.loopers.domain.waitingqueue.QueueSnapshot;

public class WaitingQueueV1Dto {

    /**
     * 대기열 진입/순번 조회 응답.
     * pollAfterSeconds: 클라이언트 권장 폴링 주기(초). 대기 많을수록 확대(Step 3, NFR-7). Step 1에선 null.
     */
    public record RankResponse(
        String status,
        Long rank,
        long aheadCount,
        long estimatedWaitSeconds,
        Integer pollAfterSeconds
    ) {
        public static RankResponse from(QueueSnapshot snapshot) {
            return new RankResponse(
                snapshot.status().name(),
                snapshot.rank(),
                snapshot.aheadCount(),
                snapshot.estimatedWaitSeconds(),
                null
            );
        }
    }
}
