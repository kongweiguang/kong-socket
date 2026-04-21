package io.github.kongweiguang.socket.tcp;

import java.net.InetSocketAddress;
import java.time.Duration;

/**
 * 客户端连接状态监听器。
 *
 * @author kongweiguang
 */
public interface TcpClientListener {

    /**
     * 每次发起连接尝试前触发。
     *
     * @param remote 远端地址
     * @param attempt 尝试次数，从 1 开始
     */
    default void onConnecting(InetSocketAddress remote, int attempt) {
    }

    /**
     * 连接尝试成功后触发。
     *
     * @param connection 已建立的连接
     */
    default void onConnected(Connection connection) {
    }

    /**
     * 连接尝试失败时触发。
     *
     * @param remote 远端地址
     * @param attempt 尝试次数
     * @param error 异常信息
     */
    default void onConnectFailed(InetSocketAddress remote, int attempt, Throwable error) {
    }

    /**
     * 准备调度下一次重连前触发。
     *
     * @param remote 远端地址
     * @param nextAttempt 下一次尝试次数
     * @param delay 重连延迟
     */
    default void onReconnectScheduled(InetSocketAddress remote, int nextAttempt, Duration delay) {
    }

    /**
     * 客户端放弃重连时触发。
     *
     * @param remote 远端地址
     * @param attempts 已使用的尝试次数
     * @param lastError 最后一次异常
     */
    default void onReconnectFailed(InetSocketAddress remote, int attempts, Throwable lastError) {
    }

    /**
     * 客户端连接断开时触发。
     *
     * @param connection 已断开的连接
     */
    default void onDisconnected(Connection connection) {
    }
}
