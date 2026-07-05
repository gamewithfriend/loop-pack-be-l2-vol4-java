package com.loopers.infrastructure.waitingqueue;

import com.loopers.domain.waitingqueue.EntryTokenRepository;
import com.loopers.domain.waitingqueue.QueueSnapshot;
import com.loopers.domain.waitingqueue.QueueStatus;
import com.loopers.domain.waitingqueue.WaitingQueueService;
import com.loopers.support.error.CoreException;
import com.loopers.utils.RedisCleanUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * 대기열 Redis 어댑터 통합 검증(실제 Redis, Testcontainers). ZSET FIFO·멱등 진입·발급 배치·토큰 검증/소모를
 * 실제 Redis 명령으로 확인한다. (산정 규칙 자체는 WaitingQueueServiceTest가 mock으로 검증)
 */
@SpringBootTest
class WaitingQueueRedisIntegrationTest {

    @Autowired WaitingQueueService service;
    @Autowired EntryTokenRepository tokens;
    @Autowired RedisCleanUp redisCleanUp;

    @AfterEach
    void tearDown() {
        redisCleanUp.truncateAll();
    }

    @DisplayName("진입 순서대로 FIFO 순번을 부여한다")
    @Test
    void fifoRank() {
        QueueSnapshot first = service.enter(101L);
        QueueSnapshot second = service.enter(102L);
        QueueSnapshot third = service.enter(103L);

        assertThat(first.status()).isEqualTo(QueueStatus.WAITING);
        assertThat(first.rank()).isEqualTo(1L);
        assertThat(second.rank()).isEqualTo(2L);
        assertThat(third.rank()).isEqualTo(3L);
    }

    @DisplayName("멱등 진입: 같은 유저가 다시 진입해도 순번이 밀리지 않는다")
    @Test
    void idempotentEnter() {
        service.enter(201L);
        service.enter(202L);

        QueueSnapshot again = service.enter(201L); // 재진입

        assertThat(again.rank()).isEqualTo(1L); // 여전히 1번
    }

    @DisplayName("issueBatch: 앞에서부터 pop해 토큰 발급 → 해당 유저는 활성(READY)이 된다")
    @Test
    void issueBatchActivatesFrontUsers() {
        service.enter(301L);
        service.enter(302L);
        service.enter(303L);

        int issued = service.issueBatch();

        assertThat(issued).isEqualTo(3);
        assertThat(tokens.isActive(301L)).isTrue();
        assertThat(tokens.activeCountLive()).isEqualTo(3L);
        // 발급된 유저는 대기열에서 빠져 READY
        assertThat(service.resolve(301L).status()).isEqualTo(QueueStatus.READY);
        // 아직 대기열에 없던 유저는 NOT_IN_QUEUE
        assertThat(service.resolve(999L).status()).isEqualTo(QueueStatus.NOT_IN_QUEUE);
    }

    @DisplayName("발급된 토큰으로 검증 통과 후 consume하면 활성 슬롯이 회수된다")
    @Test
    void validateThenConsume() {
        service.enter(401L);
        service.issueBatch();

        String token = tokens.findTokenByUser(401L);
        assertThat(token).isNotBlank();

        // 유효한 토큰 → 통과(예외 없음)
        assertThat(catchThrowable(() -> service.validateToken(401L, token))).isNull();
        // 소유자 불일치 → FORBIDDEN
        assertThat(catchThrowable(() -> service.validateToken(999L, token)))
            .isInstanceOf(CoreException.class);

        service.consume(401L, token);

        assertThat(tokens.isActive(401L)).isFalse();
        assertThat(tokens.findUserIdByToken(token)).isNull();
        assertThat(service.resolve(401L).status()).isEqualTo(QueueStatus.NOT_IN_QUEUE);
    }

    @DisplayName("status: 대기 인원·활성 인원·파생값을 반영한다")
    @Test
    void status() {
        List.of(501L, 502L, 503L, 504L).forEach(service::enter);
        service.issueBatch(); // 4명 활성화(활성 상한 30 미만)

        var status = service.status();

        assertThat(status.queueSize()).isEqualTo(0L);   // 전원 발급되어 큐 비움
        assertThat(status.activeCount()).isEqualTo(4L);
        assertThat(status.maxActive()).isEqualTo(30);
        assertThat(status.throughputPerSecond()).isEqualTo(60.0);
    }
}
