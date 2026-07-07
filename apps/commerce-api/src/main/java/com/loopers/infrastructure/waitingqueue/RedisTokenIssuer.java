package com.loopers.infrastructure.waitingqueue;

import com.loopers.config.redis.RedisConfig;
import com.loopers.domain.waitingqueue.TokenIssuer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;

/**
 * 입장 토큰 발급 배치의 원자 실행 어댑터({@link TokenIssuer}).
 *
 * <p>만료 활성 청소 → 여유분(maxActive - 활성) 계산 → 대기열 앞에서 ZPOPMIN → pass/user-pass(String+TTL) +
 * active:users(ZSET) 기록을 <b>단일 Lua 스크립트</b>로 원자 실행한다. Redis는 스크립트를 싱글스레드로 원자
 * 처리하므로, 여러 인스턴스가 동시에 호출해도 count-then-issue 사이에 끼어듦이 없어 활성 상한 초과 발급이
 * 원천 차단된다(ShedLock 등 분산 락 불필요). 정합성이 걸린 연산이라 마스터 템플릿으로만 실행한다(04 §0).
 */
@Repository
public class RedisTokenIssuer implements TokenIssuer {

    private static final String QUEUE_KEY = "waiting:queue";
    private static final String ACTIVE_KEY = "active:users";
    private static final String PASS_PREFIX = "pass:";
    private static final String USER_PASS_PREFIX = "user-pass:";

    /**
     * KEYS[1]=waiting:queue, KEYS[2]=active:users.
     * ARGV[1]=now(ms), ARGV[2]=maxActive, ARGV[3]=ttlSeconds, ARGV[4]=passPrefix, ARGV[5]=userPassPrefix,
     * ARGV[6..]=후보 토큰(발급분만 소비). 반환=발급된 userId 목록.
     */
    private static final String ISSUE_SCRIPT =
        "local now = tonumber(ARGV[1]) " +
        "local maxActive = tonumber(ARGV[2]) " +
        "local ttl = tonumber(ARGV[3]) " +
        "local passPrefix = ARGV[4] " +
        "local userPassPrefix = ARGV[5] " +
        "redis.call('ZREMRANGEBYSCORE', KEYS[2], 0, now) " +          // 만료 활성 청소
        "local active = redis.call('ZCARD', KEYS[2]) " +             // 현재 활성
        "local k = maxActive - active " +                            // 여유분
        "if k <= 0 then return {} end " +
        "local popped = redis.call('ZPOPMIN', KEYS[1], k) " +        // 앞에서 최대 k명(FIFO)
        "local expireAt = now + ttl * 1000 " +
        "local issued = {} " +
        "local n = #popped / 2 " +                                   // popped = {member, score, ...}
        "for i = 1, n do " +
        "  local userId = popped[(i - 1) * 2 + 1] " +
        "  local token = ARGV[5 + i] " +
        "  redis.call('SET', passPrefix .. token, userId, 'EX', ttl) " +
        "  redis.call('SET', userPassPrefix .. userId, token, 'EX', ttl) " +
        "  redis.call('ZADD', KEYS[2], expireAt, userId) " +
        "  issued[i] = userId " +
        "end " +
        "return issued";

    private final RedisTemplate<String, String> redis;
    private final RedisScript<List> script;

    public RedisTokenIssuer(
        @Qualifier(RedisConfig.REDIS_TEMPLATE_MASTER) RedisTemplate<String, String> masterRedisTemplate
    ) {
        this.redis = masterRedisTemplate;
        this.script = new DefaultRedisScript<>(ISSUE_SCRIPT, List.class);
    }

    @Override
    public List<Long> issueFront(int maxActive, int ttlSeconds, List<String> tokens) {
        if (maxActive <= 0) {
            return List.of();
        }
        List<String> args = new ArrayList<>(5 + tokens.size());
        args.add(Long.toString(System.currentTimeMillis()));
        args.add(Integer.toString(maxActive));
        args.add(Integer.toString(ttlSeconds));
        args.add(PASS_PREFIX);
        args.add(USER_PASS_PREFIX);
        args.addAll(tokens);

        @SuppressWarnings("unchecked")
        List<String> issued = redis.execute(script, List.of(QUEUE_KEY, ACTIVE_KEY), args.toArray());
        if (issued == null || issued.isEmpty()) {
            return List.of();
        }
        List<Long> result = new ArrayList<>(issued.size());
        for (String userId : issued) {
            result.add(Long.valueOf(userId));
        }
        return result;
    }
}
