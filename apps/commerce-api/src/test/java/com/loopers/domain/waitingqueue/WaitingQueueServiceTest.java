package com.loopers.domain.waitingqueue;

import com.loopers.config.waitingqueue.WaitingQueueProperties;
import com.loopers.support.error.CoreException;
import com.loopers.support.error.ErrorType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * WaitingQueueService 순수 단위 테스트 — Repository를 mock으로 격리해 Redis 없이
 * 멱등 진입·상태 판정·발급 배치(back-pressure)·토큰 검증/소모 규칙을 검증한다.
 */
class WaitingQueueServiceTest {

    private static final Long USER = 1001L;

    private WaitingQueueRepository queue;
    private EntryTokenRepository tokens;
    private WaitingQueueService service;

    @BeforeEach
    void setUp() {
        queue = mock(WaitingQueueRepository.class);
        tokens = mock(EntryTokenRepository.class);
        // maxActive=30, throughput=60/s
        ThroughputPolicy policy = new ThroughputPolicy(
            new WaitingQueueProperties(true, 40, 0.25, 0.5, 2, 60, 2));
        service = new WaitingQueueService(queue, tokens, policy);
    }

    @Nested
    @DisplayName("enter — 대기열 진입")
    class Enter {
        @Test
        @DisplayName("이미 활성(토큰 보유)이면 READY, 큐에 넣지 않는다")
        void alreadyActive() {
            when(tokens.isActive(USER)).thenReturn(true);

            QueueSnapshot snapshot = service.enter(USER);

            assertThat(snapshot.status()).isEqualTo(QueueStatus.READY);
            verify(queue, never()).enqueueIfAbsent(USER);
        }

        @Test
        @DisplayName("신규 진입 시 enqueue 후 WAITING + 순번/ETA 반환")
        void newEntry() {
            when(tokens.isActive(USER)).thenReturn(false);
            when(queue.rank(USER)).thenReturn(59L); // 0-based → 앞선 60명

            QueueSnapshot snapshot = service.enter(USER);

            verify(queue).enqueueIfAbsent(USER);
            assertThat(snapshot.status()).isEqualTo(QueueStatus.WAITING);
            assertThat(snapshot.rank()).isEqualTo(60L);        // 1-based
            assertThat(snapshot.aheadCount()).isEqualTo(59L);
            assertThat(snapshot.estimatedWaitSeconds()).isEqualTo(1L); // ceil(59/60)
        }

        @Test
        @DisplayName("멱등: 이미 대기 중이어도 순번 유지(enqueueIfAbsent가 false 반환해도 rank 조회)")
        void idempotent() {
            when(tokens.isActive(USER)).thenReturn(false);
            when(queue.enqueueIfAbsent(USER)).thenReturn(false);
            when(queue.rank(USER)).thenReturn(4L);

            QueueSnapshot snapshot = service.enter(USER);

            assertThat(snapshot.status()).isEqualTo(QueueStatus.WAITING);
            assertThat(snapshot.rank()).isEqualTo(5L);
        }
    }

    @Nested
    @DisplayName("resolve — 순번 조회")
    class Resolve {
        @Test
        @DisplayName("활성이면 READY")
        void ready() {
            when(tokens.isActive(USER)).thenReturn(true);
            assertThat(service.resolve(USER).status()).isEqualTo(QueueStatus.READY);
        }

        @Test
        @DisplayName("큐에 없으면 NOT_IN_QUEUE")
        void notInQueue() {
            when(tokens.isActive(USER)).thenReturn(false);
            when(queue.rank(USER)).thenReturn(null);
            assertThat(service.resolve(USER).status()).isEqualTo(QueueStatus.NOT_IN_QUEUE);
        }
    }

    @Nested
    @DisplayName("issueBatch — 발급 배치(back-pressure)")
    class IssueBatch {
        @Test
        @DisplayName("활성 여유분(batchSize)만큼만 pop해 발급한다")
        void respectsBatchSize() {
            when(tokens.purgeExpiredAndCount()).thenReturn(25L); // 활성 25 → 여유 5
            when(queue.popFront(5)).thenReturn(List.of(1L, 2L, 3L, 4L, 5L));

            int issued = service.issueBatch();

            assertThat(issued).isEqualTo(5);
            verify(queue).popFront(5);
            verify(tokens, times(5)).issue(org.mockito.ArgumentMatchers.anyLong(),
                org.mockito.ArgumentMatchers.anyString(), eq(60));
        }

        @Test
        @DisplayName("활성이 상한(30)이면 발급 0, pop 하지 않음")
        void skipWhenFull() {
            when(tokens.purgeExpiredAndCount()).thenReturn(30L);

            int issued = service.issueBatch();

            assertThat(issued).isEqualTo(0);
            verify(queue, never()).popFront(anyInt());
        }

        @Test
        @DisplayName("발급 실패한 유저는 큐 뒤로 재큐잉(유실 방지)")
        void requeueOnFailure() {
            when(tokens.purgeExpiredAndCount()).thenReturn(0L);
            when(queue.popFront(30)).thenReturn(List.of(7L, 8L));
            org.mockito.Mockito.doThrow(new RuntimeException("redis down"))
                .when(tokens).issue(eq(7L), org.mockito.ArgumentMatchers.anyString(), eq(60));

            int issued = service.issueBatch();

            assertThat(issued).isEqualTo(1); // 8L만 성공
            verify(queue).requeue(7L);
        }
    }

    @Nested
    @DisplayName("validateToken — 주문 진입 가드")
    class Validate {
        @Test
        @DisplayName("토큰 없으면 FORBIDDEN")
        void missing() {
            Throwable t = catchThrowable(() -> service.validateToken(USER, null));
            assertThat(t).isInstanceOf(CoreException.class);
            assertThat(((CoreException) t).getErrorType()).isEqualTo(ErrorType.FORBIDDEN);
        }

        @Test
        @DisplayName("존재하지 않는/만료 토큰이면 FORBIDDEN")
        void expired() {
            when(tokens.findUserIdByToken("tk")).thenReturn(null);
            Throwable t = catchThrowable(() -> service.validateToken(USER, "tk"));
            assertThat(((CoreException) t).getErrorType()).isEqualTo(ErrorType.FORBIDDEN);
        }

        @Test
        @DisplayName("소유자 불일치면 FORBIDDEN")
        void mismatch() {
            when(tokens.findUserIdByToken("tk")).thenReturn(2002L);
            Throwable t = catchThrowable(() -> service.validateToken(USER, "tk"));
            assertThat(((CoreException) t).getErrorType()).isEqualTo(ErrorType.FORBIDDEN);
        }

        @Test
        @DisplayName("유효하면 통과(예외 없음)")
        void valid() {
            when(tokens.findUserIdByToken("tk")).thenReturn(USER);
            Throwable t = catchThrowable(() -> service.validateToken(USER, "tk"));
            assertThat(t).isNull();
        }
    }

    @Test
    @DisplayName("consume — 토큰/활성 슬롯 회수 위임")
    void consume() {
        service.consume(USER, "tk");
        verify(tokens).consume(USER, "tk");
    }
}
