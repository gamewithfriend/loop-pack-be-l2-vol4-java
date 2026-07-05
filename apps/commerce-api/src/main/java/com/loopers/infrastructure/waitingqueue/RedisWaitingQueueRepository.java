package com.loopers.infrastructure.waitingqueue;

import com.loopers.config.redis.RedisConfig;
import com.loopers.domain.waitingqueue.WaitingQueueRepository;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Repository;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * 대기 순서(waiting:queue ZSET) 어댑터. 순서·중복체크·pop은 복제 지연을 허용하면 안 되므로
 * 마스터 템플릿({@link RedisConfig#REDIS_TEMPLATE_MASTER})으로만 조작한다(04 §0).
 */
@Repository
public class RedisWaitingQueueRepository implements WaitingQueueRepository {

    private static final String QUEUE_KEY = "waiting:queue";
    private static final String SEQ_KEY = "waiting:seq";

    private final RedisTemplate<String, String> redis;
    private final ZSetOperations<String, String> zset;
    private final ValueOperations<String, String> value;

    public RedisWaitingQueueRepository(
        @Qualifier(RedisConfig.REDIS_TEMPLATE_MASTER) RedisTemplate<String, String> masterRedisTemplate
    ) {
        this.redis = masterRedisTemplate;
        this.zset = masterRedisTemplate.opsForZSet();
        this.value = masterRedisTemplate.opsForValue();
    }

    @Override
    public boolean enqueueIfAbsent(Long userId) {
        String member = userId.toString();
        if (zset.score(QUEUE_KEY, member) != null) {
            return false;
        }
        long seq = nextSeq();
        Boolean added = zset.addIfAbsent(QUEUE_KEY, member, (double) seq);
        return Boolean.TRUE.equals(added);
    }

    @Override
    public Long rank(Long userId) {
        return zset.rank(QUEUE_KEY, userId.toString());
    }

    @Override
    public boolean isQueued(Long userId) {
        return zset.score(QUEUE_KEY, userId.toString()) != null;
    }

    @Override
    public long size() {
        Long size = zset.zCard(QUEUE_KEY);
        return size == null ? 0L : size;
    }

    @Override
    public List<Long> popFront(int k) {
        if (k <= 0) {
            return Collections.emptyList();
        }
        Set<ZSetOperations.TypedTuple<String>> popped = zset.popMin(QUEUE_KEY, k);
        if (popped == null || popped.isEmpty()) {
            return Collections.emptyList();
        }
        // popMin은 score 오름차순(FIFO) 보장. value(userId)만 추출.
        return popped.stream()
            .map(ZSetOperations.TypedTuple::getValue)
            .filter(v -> v != null)
            .map(Long::valueOf)
            .toList();
    }

    @Override
    public void requeue(Long userId) {
        long seq = nextSeq();
        zset.addIfAbsent(QUEUE_KEY, userId.toString(), (double) seq);
    }

    private long nextSeq() {
        Long seq = value.increment(SEQ_KEY);
        return seq == null ? System.nanoTime() : seq;
    }
}
