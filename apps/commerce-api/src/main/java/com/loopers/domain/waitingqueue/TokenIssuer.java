package com.loopers.domain.waitingqueue;

import java.util.List;

/**
 * 방류형 입장 토큰 발급의 <b>원자 실행</b> 포트. 구현은 Redis Lua 스크립트 —
 * 고정 윈도우 레이트리밋(M초 윈도우당 N명) → 대기열 앞에서 pop → pass/user-pass/active 기록을
 * 한 번의 원자 연산으로 처리한다(docs/week8/04-redis-model.md §2.6).
 *
 * <p>Redis가 스크립트를 싱글스레드로 원자 처리하고, 윈도우 카운터가 방류량을 M초당 N명으로 묶으므로
 * 다중 인스턴스가 동시에 호출해도 주기당 방류 총합이 N을 넘지 않는다(분산 락 불필요).
 */
public interface TokenIssuer {

    /**
     * 이번 주기 방류 여유분(= N − 이번 윈도우에서 이미 방류한 수)만큼 대기열 앞에서 원자적으로 pop·발급한다.
     *
     * @param releaseSize     N. 한 주기(윈도우)에 방류할 최대 인원.
     * @param intervalSeconds M. 방류 주기(초) = 레이트리밋 윈도우 크기.
     * @param ttlSeconds      발급 토큰 TTL(초).
     * @param tokens          후보 토큰. 최소 {@code releaseSize}개여야 하며, 실제 방류분(≤ 여유분)만 소비된다.
     * @return 이번 배치에서 방류된 userId 목록(방류 인원 = size).
     */
    List<Long> issueFront(int releaseSize, int intervalSeconds, int ttlSeconds, List<String> tokens);
}
