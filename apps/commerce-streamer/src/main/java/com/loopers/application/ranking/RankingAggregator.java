package com.loopers.application.ranking;

import com.fasterxml.jackson.databind.JsonNode;
import com.loopers.application.metrics.EventEnvelope;
import com.loopers.infrastructure.ranking.RankingKey;
import com.loopers.infrastructure.ranking.RankingRedisRepository;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * catalog-events / order-events 배치를 받아 일간 랭킹 ZSET({@code ranking:all:{yyyyMMdd}})에 가중 점수를 누적한다.
 * product_metrics DB 집계({@code MetricsAggregator})와 <b>별개 컨슈머 그룹</b>으로 같은 토픽을 소비해,
 * 랭킹 파이프라인(Redis)과 측정값 파이프라인(DB)이 서로의 장애/오프셋에 독립적이다.
 *
 * <p><b>배치 내 coalescing</b>: {@code (일간 키, productId)} 단위로 점수 델타를 메모리에서 합산해 상품당 ZINCRBY 를
 * 1회로 줄인다(hot key 완화, 가산=교환법칙이라 순서 무관). 일자는 이벤트 발생시각 기준이라 자정 근처 이벤트도
 * 올바른 날짜 버킷에 들어간다.
 *
 * <p>eventType 별 해석:
 * <ul>
 *   <li>{@code PRODUCT_VIEWED} payload {productId}                    → +조회 스코어</li>
 *   <li>{@code LIKE_CHANGED}   payload {productId, delta}             → ±좋아요 스코어</li>
 *   <li>{@code ORDER_PAID}     payload {items:[{productId, quantity, unitPrice}]} → +주문(매출) 스코어</li>
 * </ul>
 * 그 외 eventType 은 랭킹과 무관하므로 조용히 스킵한다.
 */
@Component
@RequiredArgsConstructor
public class RankingAggregator {

    public static final String CONSUMER_GROUP = "ranking-aggregator";

    private static final Logger log = LoggerFactory.getLogger(RankingAggregator.class);

    private final RankingRedisRepository rankingRedisRepository;

    public void apply(List<EventEnvelope> envelopes) {
        if (envelopes == null || envelopes.isEmpty()) {
            return;
        }

        // key(일간) -> (productId(member) -> scoreDelta) 배치 내 합산
        Map<String, Map<String, Double>> deltasByKey = new HashMap<>();

        for (EventEnvelope e : envelopes) {
            if (e.eventType() == null || e.payload() == null) {
                continue;
            }
            LocalDate date = RankingKey.dateOf(e.occurredAt());
            String key = RankingKey.daily(date);
            JsonNode payload = e.payload();

            switch (e.eventType()) {
                case "PRODUCT_VIEWED" -> add(deltasByKey, key,
                        payload.get("productId").asLong(), RankingScorePolicy.viewScore());
                case "LIKE_CHANGED" -> add(deltasByKey, key,
                        payload.get("productId").asLong(), RankingScorePolicy.likeScore(payload.get("delta").asLong()));
                case "ORDER_PAID" -> {
                    for (JsonNode item : payload.get("items")) {
                        long productId = item.get("productId").asLong();
                        long quantity = item.get("quantity").asLong();
                        // unitPrice 는 week8 에 추가된 필드 — 구(舊) 이벤트 호환을 위해 없으면 0(가산 없음)
                        long unitPrice = item.hasNonNull("unitPrice") ? item.get("unitPrice").asLong() : 0L;
                        add(deltasByKey, key, productId, RankingScorePolicy.orderScore(unitPrice, quantity));
                    }
                }
                default -> {
                    // 랭킹과 무관한 이벤트 스킵
                }
            }
        }

        rankingRedisRepository.incrementAll(deltasByKey);
        log.debug("랭킹 집계: envelopes={}, keys={}", envelopes.size(), deltasByKey.size());
    }

    private void add(Map<String, Map<String, Double>> deltasByKey, String key, long productId, double score) {
        if (score == 0.0) {
            return;
        }
        deltasByKey.computeIfAbsent(key, k -> new HashMap<>())
                .merge(String.valueOf(productId), score, Double::sum);
    }
}
