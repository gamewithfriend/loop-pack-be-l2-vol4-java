package com.loopers.application.ranking;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * 스코어 정책 단위 테스트 — 이벤트별 가산치(Weight × Score)와 매출 log 정규화를 검증한다.
 */
class RankingScorePolicyTest {

    @DisplayName("조회 스코어는 VIEW_WEIGHT(0.1)이다.")
    @Test
    void viewScore() {
        assertThat(RankingScorePolicy.viewScore()).isEqualTo(0.1);
    }

    @Nested
    @DisplayName("좋아요 스코어는 LIKE_WEIGHT × delta 로, 취소(-1)는 감점된다.")
    class LikeScore {
        @Test
        void plusOne() {
            assertThat(RankingScorePolicy.likeScore(1)).isCloseTo(0.2, within(1e-9));
        }

        @Test
        void minusOne_감점() {
            assertThat(RankingScorePolicy.likeScore(-1)).isCloseTo(-0.2, within(1e-9));
        }

        @Test
        void multipleDelta() {
            assertThat(RankingScorePolicy.likeScore(3)).isCloseTo(0.6, within(1e-9));
        }
    }

    @Nested
    @DisplayName("주문 스코어는 ORDER_WEIGHT × log10(1 + 단가×수량) 이다.")
    class OrderScore {
        @Test
        void 매출_log정규화() {
            // 10,000원 × 2 = 20,000 매출
            double expected = 0.6 * Math.log10(1 + 20_000.0);
            assertThat(RankingScorePolicy.orderScore(10_000, 2)).isCloseTo(expected, within(1e-9));
        }

        @Test
        void 고가상품도_log로_눌린다() {
            // 매출이 100배(2만→200만) 늘어도 가산치는 log 스케일이라 ~1.5배 미만 증가
            double small = RankingScorePolicy.orderScore(10_000, 2);      // 매출 2만
            double large = RankingScorePolicy.orderScore(1_000_000, 2);   // 매출 200만
            assertThat(large).isGreaterThan(small);
            assertThat(large / small).isLessThan(2.0);
        }

        @Test
        void 매출0이하는_가산하지_않는다() {
            assertThat(RankingScorePolicy.orderScore(0, 5)).isEqualTo(0.0);
            assertThat(RankingScorePolicy.orderScore(1_000, 0)).isEqualTo(0.0);
        }
    }
}
