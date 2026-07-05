package com.loopers.domain.waitingqueue;

import com.loopers.config.waitingqueue.WaitingQueueProperties;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 토큰 TTL 스윕 시뮬레이션(부하테스트 대체, docs/week8 §D1 튜닝 근거).
 *
 * <p>실제 정책 로직(maxActive·배치=활성까지 리필)을 {@link ThroughputPolicy}로 그대로 돌리되,
 * "발급된 토큰이 안 쓰이는" 상황(이탈·느린 주문)을 모델링해 TTL이 처리량/UX에 미치는 영향을 본다.
 * TTL은 이탈자가 슬롯을 점유하는 시간이자, 느린 정상 유저를 바운스시키는 마감이다.
 *
 * <p>공정 비교를 위해 유저 특성(이탈 여부·주문 지연)을 고정 시드로 한 번만 뽑고 모든 TTL 런에서 재사용한다.
 * 결과는 System.out(JUnit system-out)으로 표를 출력한다. 결정적(시드 고정)이라 재현 가능.
 */
class WaitingQueueTtlSimulationTest {

    // ---- 시나리오 파라미터 ----
    private static final int N = 5_000;              // 플래시 크라우드 인원(t=0 동시 진입)
    private static final int MAX_ACTIVE = 30;        // = ThroughputPolicy.maxActive() (pool40·reserve0.25)
    private static final long TICK_MS = 2_000;       // 스케줄러 주기
    private static final long DT_MS = 250;           // 시뮬레이션 시간 스텝
    private static final double P_ABANDON = 0.15;    // "탭 닫는" 이탈자 비율
    private static final double LATENCY_MEAN_MS = 6_000; // 정상 유저 주문 지연 평균(지수분포)
    private static final int MAX_RETRIES = 5;        // 바운스 후 재진입 상한(초과 시 실패)
    private static final long SAFETY_LIMIT_MS = 6L * 3600 * 1000;
    private static final int[] TTL_SWEEP_SEC = {8, 15, 30, 60, 120};
    private static final long SEED = 42L;

    private static final class User {
        final boolean abandon;
        final long latencyMs;   // 이탈자면 무의미
        long enterMs;           // 최초 진입 시각(대기 측정 기준, 재진입해도 유지)
        int retries;
        User(boolean abandon, long latencyMs) {
            this.abandon = abandon;
            this.latencyMs = latencyMs;
        }
    }

    private static final class Token {
        final int userIdx;
        final long expireMs;
        final long orderMs; // 정상 유저의 주문 시도 시각(=admit+latency), 이탈자는 Long.MAX
        Token(int userIdx, long expireMs, long orderMs) {
            this.userIdx = userIdx;
            this.expireMs = expireMs;
            this.orderMs = orderMs;
        }
    }

    @Test
    @DisplayName("TTL 스윕: 처리량/이탈낭비/바운스 트레이드오프 표 출력")
    void sweepTokenTtl() {
        // maxActive가 실제 정책값과 일치하는지 확인(문서 D2)
        ThroughputPolicy policy = new ThroughputPolicy(
            new WaitingQueueProperties(true, 40, 0.25, 0.5, 2, 60, 2));
        assertThat(policy.maxActive()).isEqualTo(MAX_ACTIVE);

        // 유저 특성 1회 생성 → 모든 TTL 런에서 재사용(공정 비교)
        boolean[] abandon = new boolean[N];
        long[] latency = new long[N];
        Random gen = new Random(SEED);
        for (int i = 0; i < N; i++) {
            abandon[i] = gen.nextDouble() < P_ABANDON;
            latency[i] = expSample(gen, LATENCY_MEAN_MS);
        }

        StringBuilder out = new StringBuilder();
        out.append(String.format(Locale.ROOT,
            "%nTTL 스윕 시뮬레이션 (N=%d, maxActive=%d, tick=%.0fs, 이탈=%.0f%%, 지연평균=%.0fs, seed=%d)%n",
            N, MAX_ACTIVE, TICK_MS / 1000.0, P_ABANDON * 100, LATENCY_MEAN_MS / 1000, SEED));
        out.append("─".repeat(94)).append('\n');
        out.append(String.format(Locale.ROOT,
            "%5s │ %9s │ %9s │ %8s │ %10s │ %10s │ %9s │ %8s%n",
            "TTL", "완료", "이탈", "실패", "발급수", "처리량/s", "p95대기", "배수시간"));
        out.append(String.format(Locale.ROOT,
            "%5s │ %9s │ %9s │ %8s │ %10s │ %10s │ %9s │ %8s%n",
            "(s)", "(주문)", "(탭닫음)", "(TTL초과)", "(토큰)", "(orders)", "(s)", "(min)"));
        out.append("─".repeat(94)).append('\n');

        Result best = null;
        for (int ttlSec : TTL_SWEEP_SEC) {
            Result r = run(ttlSec, abandon, latency);
            out.append(String.format(Locale.ROOT,
                "%5d │ %9d │ %9d │ %8d │ %10d │ %10.2f │ %9.1f │ %8.1f%n",
                ttlSec, r.completed, r.abandoned, r.failed, r.admissions,
                r.throughputPerSec, r.p95WaitSec, r.drainSec / 60.0));

            // 보존 법칙: 모든 유저는 완료/이탈/실패 중 하나로 귀결
            assertThat(r.completed + r.abandoned + r.failed).isEqualTo(N);
            if (best == null || r.drainSec < best.drainSec) {
                best = r;
            }
        }
        out.append("─".repeat(94)).append('\n');
        out.append(String.format(Locale.ROOT,
            "→ 배수시간 최소: TTL=%ds (%.1f분). 처리량 %.2f/s, 실패 %d명, 발급배수 %.2f×%n",
            best.ttlSec, best.drainSec / 60.0, best.throughputPerSec, best.failed,
            (double) best.admissions / Math.max(1, best.completed)));
        out.append("해석: TTL↑ → 이탈자 슬롯 점유 시간↑(처리량↓·배수시간↑). ")
            .append("TTL↓ → 느린 정상 유저 바운스/실패↑. 이탈률 하에서 배수시간을 최소화하는 TTL이 존재.\n");

        System.out.println(out);
    }

    private Result run(int ttlSec, boolean[] abandon, long[] latency) {
        long ttlMs = ttlSec * 1000L;
        User[] users = new User[N];
        ArrayDeque<Integer> queue = new ArrayDeque<>(N);
        for (int i = 0; i < N; i++) {
            users[i] = new User(abandon[i], latency[i]);
            users[i].enterMs = 0;
            queue.addLast(i);
        }
        List<Token> active = new ArrayList<>(MAX_ACTIVE + 4);
        List<Long> waits = new ArrayList<>(N);

        int completed = 0, abandoned = 0, failed = 0;
        long admissions = 0;
        long now = 0;

        while (now <= SAFETY_LIMIT_MS) {
            // 1) 활성 토큰 처리(주문 성공 / 만료)
            for (int t = active.size() - 1; t >= 0; t--) {
                Token tok = active.get(t);
                User u = users[tok.userIdx];
                if (!u.abandon && tok.orderMs <= now && tok.orderMs <= tok.expireMs) {
                    // 주문 성공 → 슬롯 회수
                    completed++;
                    waits.add(now - u.enterMs);
                    active.remove(t);
                } else if (now >= tok.expireMs) {
                    // 만료 → 슬롯 회수
                    active.remove(t);
                    if (u.abandon) {
                        abandoned++; // 이탈자는 떠남
                    } else {
                        // 느린 정상 유저 바운스 → 재진입(공정성) 또는 재시도 소진 시 실패
                        u.retries++;
                        if (u.retries > MAX_RETRIES) {
                            failed++;
                        } else {
                            queue.addLast(tok.userIdx);
                        }
                    }
                }
            }
            // 2) 스케줄러 틱: 활성 상한까지 리필
            if (now % TICK_MS == 0) {
                int batch = MAX_ACTIVE - active.size();
                for (int j = 0; j < batch && !queue.isEmpty(); j++) {
                    int idx = queue.pollFirst();
                    User u = users[idx];
                    long expireMs = now + ttlMs;
                    long orderMs = u.abandon ? Long.MAX_VALUE : now + u.latencyMs;
                    active.add(new Token(idx, expireMs, orderMs));
                    admissions++;
                }
            }
            if (queue.isEmpty() && active.isEmpty()) {
                break;
            }
            now += DT_MS;
        }

        Result r = new Result();
        r.ttlSec = ttlSec;
        r.completed = completed;
        r.abandoned = abandoned;
        r.failed = failed;
        r.admissions = admissions;
        r.drainSec = now / 1000.0;
        r.throughputPerSec = completed / Math.max(1.0, r.drainSec);
        r.p95WaitSec = percentile(waits, 95) / 1000.0;
        return r;
    }

    private static long expSample(Random gen, double meanMs) {
        double u = Math.max(1e-9, gen.nextDouble());
        return (long) (-meanMs * Math.log(u));
    }

    private static long percentile(List<Long> values, int p) {
        if (values.isEmpty()) {
            return 0;
        }
        List<Long> sorted = new ArrayList<>(values);
        sorted.sort(Long::compareTo);
        int idx = (int) Math.ceil(p / 100.0 * sorted.size()) - 1;
        return sorted.get(Math.max(0, Math.min(sorted.size() - 1, idx)));
    }

    private static final class Result {
        int ttlSec;
        int completed;
        int abandoned;
        int failed;
        long admissions;
        double drainSec;
        double throughputPerSec;
        double p95WaitSec;
    }
}
