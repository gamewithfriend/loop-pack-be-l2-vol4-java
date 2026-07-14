package com.loopers.application.ranking;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.loopers.application.metrics.EventEnvelope;
import com.loopers.infrastructure.ranking.RankingRedisRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * 랭킹 집계 로직 단위 테스트 — Kafka/Redis 없이 검증한다.
 * eventType 별 스코어 해석 + 배치 내 (일간 키, productId) coalescing + 일자 버킷 분기가 핵심.
 */
class RankingAggregatorTest {

    private static final JsonNodeFactory J = JsonNodeFactory.instance;
    private static final String KST_MIDNIGHT = "2026-07-14T00:00:00+09:00"; // → ranking:all:20260714
    private static final String KEY = "ranking:all:20260714";

    private EventEnvelope viewed(long productId, String occurredAt) {
        ObjectNode p = J.objectNode();
        p.put("productId", productId);
        return new EventEnvelope(1L, "PRODUCT_VIEWED", "product", productId, 0L, occurredAt, p);
    }

    private EventEnvelope like(long productId, long delta, String occurredAt) {
        ObjectNode p = J.objectNode();
        p.put("productId", productId);
        p.put("delta", delta);
        return new EventEnvelope(2L, "LIKE_CHANGED", "product", productId, 0L, occurredAt, p);
    }

    private EventEnvelope orderPaid(long productId, int quantity, long unitPrice, String occurredAt) {
        ObjectNode p = J.objectNode();
        p.put("orderId", 100L);
        ArrayNode items = p.putArray("items");
        ObjectNode item = items.addObject();
        item.put("productId", productId);
        item.put("quantity", quantity);
        item.put("unitPrice", unitPrice);
        return new EventEnvelope(3L, "ORDER_PAID", "order", 100L, 0L, occurredAt, p);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Map<String, Double>> capture(RankingRedisRepository repo) {
        ArgumentCaptor<Map<String, Map<String, Double>>> captor = ArgumentCaptor.forClass(Map.class);
        verify(repo).incrementAll(captor.capture());
        return captor.getValue();
    }

    @DisplayName("eventType별 스코어를 (일간 키, productId)로 합산해 incrementAll에 넘긴다.")
    @Test
    void aggregatesByType() {
        RankingRedisRepository repo = mock(RankingRedisRepository.class);
        RankingAggregator aggregator = new RankingAggregator(repo);

        // 상품10: 조회 +0.1, 조회 +0.1, 좋아요 +0.2 = +0.4 (coalescing)
        // 상품20: 좋아요 취소 -0.2
        // 상품30: 주문(10,000 × 2) = 0.6·log10(1+20000)
        aggregator.apply(List.of(
                viewed(10, KST_MIDNIGHT),
                viewed(10, KST_MIDNIGHT),
                like(10, +1, KST_MIDNIGHT),
                like(20, -1, KST_MIDNIGHT),
                orderPaid(30, 2, 10_000, KST_MIDNIGHT)
        ));

        Map<String, Map<String, Double>> deltas = capture(repo);
        assertThat(deltas).containsOnlyKeys(KEY);
        Map<String, Double> byProduct = deltas.get(KEY);
        assertThat(byProduct).containsOnlyKeys("10", "20", "30");
        assertThat(byProduct.get("10")).isCloseTo(0.4, within(1e-9));   // 두 조회 + 좋아요 합산
        assertThat(byProduct.get("20")).isCloseTo(-0.2, within(1e-9));  // 좋아요 취소 감점
        assertThat(byProduct.get("30")).isCloseTo(0.6 * Math.log10(1 + 20_000.0), within(1e-9));
    }

    @DisplayName("발생시각(occurredAt)이 다른 날이면 서로 다른 일간 키로 분리된다.")
    @Test
    void splitsByDateBucket() {
        RankingRedisRepository repo = mock(RankingRedisRepository.class);
        RankingAggregator aggregator = new RankingAggregator(repo);

        aggregator.apply(List.of(
                viewed(10, "2026-07-14T00:00:00+09:00"),           // → 20260714
                viewed(10, "2026-07-13T23:59:59+09:00")            // → 20260713
        ));

        Map<String, Map<String, Double>> deltas = capture(repo);
        assertThat(deltas).containsOnlyKeys("ranking:all:20260714", "ranking:all:20260713");
        assertThat(deltas.get("ranking:all:20260714").get("10")).isCloseTo(0.1, within(1e-9));
        assertThat(deltas.get("ranking:all:20260713").get("10")).isCloseTo(0.1, within(1e-9));
    }

    @DisplayName("빈 배치는 아무것도 반영하지 않는다.")
    @Test
    void noOpWhenEmpty() {
        RankingRedisRepository repo = mock(RankingRedisRepository.class);
        RankingAggregator aggregator = new RankingAggregator(repo);

        aggregator.apply(List.of());

        verify(repo, never()).incrementAll(any());
    }

    @DisplayName("알 수 없는 eventType은 스킵한다(랭킹 무관 이벤트).")
    @Test
    void skipsUnknownType() {
        RankingRedisRepository repo = mock(RankingRedisRepository.class);
        RankingAggregator aggregator = new RankingAggregator(repo);

        ObjectNode p = J.objectNode();
        p.put("couponId", 1L);
        EventEnvelope unknown = new EventEnvelope(9L, "COUPON_ISSUE_REQUESTED", "coupon", 1L, 0L, KST_MIDNIGHT, p);

        aggregator.apply(List.of(unknown, viewed(10, KST_MIDNIGHT)));

        Map<String, Map<String, Double>> deltas = capture(repo);
        assertThat(deltas.get(KEY)).containsOnlyKeys("10"); // 조회만 반영, 쿠폰 이벤트 무시
    }
}
