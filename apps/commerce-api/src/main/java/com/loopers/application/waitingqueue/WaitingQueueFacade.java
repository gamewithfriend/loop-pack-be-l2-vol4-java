package com.loopers.application.waitingqueue;

import com.loopers.config.waitingqueue.WaitingQueueProperties;
import com.loopers.domain.waitingqueue.QueueSnapshot;
import com.loopers.domain.waitingqueue.WaitingQueueService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

/**
 * 대기열 대고객 유스케이스 조립. (Step 3에서 순번 조회 앞단에 RankCache를 붙인다.)
 *
 * <p>주문 API 진입 가드(validateEntry/releaseEntry)는 {@code waiting-queue.enabled} 플래그로 토글한다.
 * 평시(false)엔 no-op이라 기존 주문 흐름을 바꾸지 않고, 블랙프라이데이(true)에만 토큰을 요구한다.
 */
@RequiredArgsConstructor
@Component
public class WaitingQueueFacade {

    private final WaitingQueueService service;
    private final WaitingQueueProperties properties;

    public QueueSnapshot enter(Long userId) {
        return service.enter(userId);
    }

    public QueueSnapshot getRank(Long userId) {
        return service.resolve(userId);
    }

    /** 주문 진입 시 토큰 검증(FR-5). 관문 비활성이면 통과. 유효하지 않으면 CoreException(FORBIDDEN). */
    public void validateEntry(Long userId, String token) {
        if (!properties.enabled()) {
            return;
        }
        service.validateToken(userId, token);
    }

    /** 주문 성공 확정 시 토큰 소모(D3). 관문 비활성이면 no-op. */
    public void releaseEntry(Long userId, String token) {
        if (!properties.enabled() || token == null || token.isBlank()) {
            return;
        }
        service.consume(userId, token);
    }
}
