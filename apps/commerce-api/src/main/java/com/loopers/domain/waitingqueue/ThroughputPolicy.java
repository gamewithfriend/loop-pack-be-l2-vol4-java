package com.loopers.domain.waitingqueue;

import com.loopers.config.waitingqueue.WaitingQueueProperties;
import org.springframework.stereotype.Component;

/**
 * 처리량·배치·ETA 산정(docs/week8 §NFR-4·D2 코드화).
 *
 * <pre>
 * maxActive          = floor(dbPoolSize * (1 - reserveRatio))   // 동시 처리 상한(back-pressure)
 * throughputPerSecond= maxActive / avgProcessSeconds            // 주문/초
 * batchSize(active)  = max(0, maxActive - active)               // 활성까지만 리필(P-5)
 * estimatedWaitSec(ahead) = ceil(ahead / throughputPerSecond)   // 예상 대기 시간
 * </pre>
 */
@Component
public class ThroughputPolicy {

    private final WaitingQueueProperties props;

    public ThroughputPolicy(WaitingQueueProperties props) {
        this.props = props;
    }

    public int maxActive() {
        return (int) Math.floor(props.dbPoolSize() * (1.0 - props.reserveRatio()));
    }

    public double throughputPerSecond() {
        double avg = props.avgProcessSeconds();
        if (avg <= 0) {
            return maxActive();
        }
        return maxActive() / avg;
    }

    /** 이번 주기에 발급할 인원 = 활성 상한까지의 여유분. 활성이 꽉 차면 0. */
    public int batchSize(long activeCount) {
        return (int) Math.max(0L, maxActive() - activeCount);
    }

    public long estimatedWaitSeconds(long aheadCount) {
        double tps = throughputPerSecond();
        if (tps <= 0 || aheadCount <= 0) {
            return 0L;
        }
        return (long) Math.ceil(aheadCount / tps);
    }

    public int tokenTtlSeconds() {
        return props.tokenTtlSeconds();
    }
}
