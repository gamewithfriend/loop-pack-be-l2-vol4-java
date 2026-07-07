package com.loopers.domain.waitingqueue;

import java.util.List;

/**
 * 입장 토큰 발급 배치의 <b>원자 실행</b> 포트. 구현은 Redis Lua 스크립트 —
 * 만료 활성 청소 → 여유분 계산(maxActive - 활성) → 대기열 앞에서 pop → pass/user-pass/active 기록을
 * 한 번의 원자 연산으로 처리한다(docs/week8/04-redis-model.md §2.6).
 *
 * <p>Redis가 스크립트를 싱글스레드로 원자 처리하므로, 다중 인스턴스가 동시에 호출해도
 * 활성 상한(maxActive) 초과 발급이 발생하지 않는다(분산 락 불필요).
 */
public interface TokenIssuer {

    /**
     * 활성 여유분만큼 대기열 앞에서 원자적으로 pop·발급한다.
     *
     * @param maxActive  동시 활성 상한. 발급 후 활성 인원이 이 값을 넘지 않는다.
     * @param ttlSeconds 발급 토큰 TTL(초).
     * @param tokens     후보 토큰. 최소 {@code maxActive}개여야 하며, 실제 발급분(≤ 여유분)만 소비된다.
     * @return 이번 배치에서 발급된 userId 목록(발급 인원 = size).
     */
    List<Long> issueFront(int maxActive, int ttlSeconds, List<String> tokens);
}
