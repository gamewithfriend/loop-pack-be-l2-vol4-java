package com.loopers.config.waitingqueue;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * 대기열(Virtual Waiting Room) 설정. 값의 근거는 docs/week8/01-requirements.md §9(D1·D2)·§NFR-4.
 *
 * <p>모든 파생값(maxActive/throughput/batchSize/eta)은 {@link com.loopers.domain.waitingqueue.ThroughputPolicy}
 * 에서 이 프로퍼티로부터 계산한다. 부하테스트 결과에 따라 코드 변경 없이 yml로 튜닝한다.
 */
@ConfigurationProperties(prefix = "waiting-queue")
public record WaitingQueueProperties(
    /** 대기열 관문 활성화. false면 주문 API가 기존처럼 토큰 없이 동작(평시). true면 블랙프라이데이 관문 on. */
    @DefaultValue("false") boolean enabled,
    /** DB 커넥션 풀 크기(측정값, HikariCP maximum-pool-size). */
    @DefaultValue("40") int dbPoolSize,
    /** 주문 외 트래픽(조회/릴레이/리컨사일)용 헤드룸 비율. maxActive = floor(dbPoolSize * (1 - reserveRatio)). */
    @DefaultValue("0.25") double reserveRatio,
    /** 주문 1건 서버 처리 평균 시간(초). throughput = maxActive / avgProcessSeconds. */
    @DefaultValue("0.5") double avgProcessSeconds,
    /** 스케줄러 주기(초). */
    @DefaultValue("2") int schedulerIntervalSeconds,
    /** 입장 토큰 TTL(초, D1). */
    @DefaultValue("60") int tokenTtlSeconds,
    /** 순번 조회 결과 캐시 TTL(초, D5). */
    @DefaultValue("2") int rankCacheSeconds
) {
}
