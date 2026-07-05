package com.loopers.interfaces.scheduling;

import com.loopers.config.waitingqueue.WaitingQueueProperties;
import com.loopers.domain.waitingqueue.WaitingQueueService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.core.LockAssert;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * 입장 토큰 발급 배치(FR-3). 매 주기 활성 여유분만큼 대기열 앞에서 pop해 토큰을 발급한다.
 * 다중 인스턴스 중복 실행은 ShedLock으로 차단(NFR-5). 관문 비활성(enabled=false, 평시)이면 no-op.
 *
 * <p>주기는 {@code waiting-queue.scheduler-interval-seconds}(초) × 1000ms. 값 근거: docs/week8 §D2.
 */
@Slf4j
@Profile("!test")
@RequiredArgsConstructor
@Component
public class EntryTokenScheduler {

    private final WaitingQueueService waitingQueueService;
    private final WaitingQueueProperties properties;

    @Scheduled(
        fixedDelayString = "${waiting-queue.scheduler-interval-seconds:2}000",
        initialDelayString = "5000")
    @SchedulerLock(
        name = "waitingQueueTokenIssue",
        lockAtMostFor = "PT10S",
        lockAtLeastFor = "PT1S")
    public void issueTokens() {
        LockAssert.assertLocked();
        if (!properties.enabled()) {
            return;
        }
        int issued = waitingQueueService.issueBatch();
        if (issued > 0) {
            log.info("입장 토큰 발급 배치: {}명 입장", issued);
        }
    }
}
