package com.loopers.domain.waitingqueue;

import com.loopers.config.waitingqueue.WaitingQueueProperties;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * ThroughputPolicy 산정 규칙 검증(docs/week8 §NFR-4·D2).
 * dbPoolSize=40, reserveRatio=0.25, avgProcessSeconds=0.5 기준
 * → maxActive=30, throughput=60/s.
 */
class ThroughputPolicyTest {

    private ThroughputPolicy policy(int pool, double reserve, double avg) {
        WaitingQueueProperties props = new WaitingQueueProperties(
            true, pool, reserve, avg, 2, 60, 2);
        return new ThroughputPolicy(props);
    }

    @Test
    @DisplayName("maxActive = floor(dbPoolSize * (1 - reserveRatio))")
    void maxActive() {
        assertThat(policy(40, 0.25, 0.5).maxActive()).isEqualTo(30);
        assertThat(policy(40, 0.0, 0.5).maxActive()).isEqualTo(40);
        assertThat(policy(10, 0.25, 0.5).maxActive()).isEqualTo(7); // floor(7.5)
    }

    @Test
    @DisplayName("throughput = maxActive / avgProcessSeconds")
    void throughput() {
        assertThat(policy(40, 0.25, 0.5).throughputPerSecond()).isEqualTo(60.0);
        assertThat(policy(40, 0.25, 1.0).throughputPerSecond()).isEqualTo(30.0);
    }

    @Test
    @DisplayName("batchSize = max(0, maxActive - active) — 활성까지만 리필, 꽉 차면 0")
    void batchSize() {
        ThroughputPolicy p = policy(40, 0.25, 0.5); // maxActive 30
        assertThat(p.batchSize(0)).isEqualTo(30);
        assertThat(p.batchSize(25)).isEqualTo(5);
        assertThat(p.batchSize(30)).isEqualTo(0);
        assertThat(p.batchSize(35)).isEqualTo(0); // 음수 방지
    }

    @Test
    @DisplayName("estimatedWaitSeconds = ceil(ahead / throughput)")
    void eta() {
        ThroughputPolicy p = policy(40, 0.25, 0.5); // throughput 60/s
        assertThat(p.estimatedWaitSeconds(0)).isEqualTo(0);
        assertThat(p.estimatedWaitSeconds(60)).isEqualTo(1);
        assertThat(p.estimatedWaitSeconds(61)).isEqualTo(2);   // ceil
        assertThat(p.estimatedWaitSeconds(600)).isEqualTo(10);
    }
}
