package io.github.kongweiguang.socket.tcp;

import java.time.Duration;

/**
 * 客户端重连策略配置。
 *
 * @author kongweiguang
 */
public final class ReconnectConfig {

    private static final ReconnectConfig DISABLED = new ReconnectConfig(false, 0, Duration.ZERO, Duration.ZERO);

    private final boolean enabled;
    private final int maxAttempts;
    private final Duration initialDelay;
    private final Duration maxDelay;

    private ReconnectConfig(boolean enabled, int maxAttempts, Duration initialDelay, Duration maxDelay) {
        this.enabled = enabled;
        this.maxAttempts = maxAttempts;
        this.initialDelay = initialDelay;
        this.maxDelay = maxDelay;
    }

    /**
     * 获取禁用重连的配置。
     *
     * @return 禁用重连配置
     */
    public static ReconnectConfig disabled() {
        return DISABLED;
    }

    /**
     * 创建重连配置。
     *
     * @param maxAttempts 首次连接失败后的最大重连次数
     * @param initialDelay 初始重连延迟
     * @param maxDelay 最大重连延迟
     * @return 重连配置
     */
    public static ReconnectConfig of(int maxAttempts, Duration initialDelay, Duration maxDelay) {
        if (maxAttempts < 0) {
            throw new IllegalArgumentException("maxAttempts must be >= 0");
        }
        if (initialDelay == null || initialDelay.isNegative()) {
            throw new IllegalArgumentException("initialDelay must be >= 0");
        }
        if (maxDelay == null || maxDelay.compareTo(initialDelay) < 0) {
            throw new IllegalArgumentException("maxDelay must be >= initialDelay");
        }
        return new ReconnectConfig(maxAttempts > 0, maxAttempts, initialDelay, maxDelay);
    }

    /**
     * 判断是否启用重连。
     *
     * @return 启用重连时返回 true
     */
    public boolean enabled() {
        return enabled;
    }

    /**
     * 获取首次连接失败后的最大重连次数。
     *
     * @return 最大重连次数
     */
    public int maxAttempts() {
        return maxAttempts;
    }

    /**
     * 获取初始重连延迟。
     *
     * @return 初始重连延迟
     */
    public Duration initialDelay() {
        return initialDelay;
    }

    /**
     * 获取最大重连延迟。
     *
     * @return 最大重连延迟
     */
    public Duration maxDelay() {
        return maxDelay;
    }

    /**
     * 计算指定重连次数对应的延迟。
     *
     * @param reconnectAttempt 重连尝试次数
     * @return 本次重连延迟
     */
    public Duration delayFor(int reconnectAttempt) {
        if (reconnectAttempt <= 1) {
            return initialDelay;
        }
        long multiplied = initialDelay.toMillis() * (1L << Math.min(reconnectAttempt - 1, 30));
        long capped = Math.min(multiplied, maxDelay.toMillis());
        return Duration.ofMillis(capped);
    }
}
