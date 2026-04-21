package io.github.kongweiguang.socket.tcp.internal.aio;

/**
 * AIO 连接关闭生命周期回调。
 *
 * @author kongweiguang
 */
interface AioConnectionLifecycle {

    /**
     * 连接关闭后触发。
     *
     * @param connection 已关闭的连接
     * @param cause 关闭原因，主动关闭时可能为 null
     */
    void onClosed(AioConnection connection, Throwable cause);
}
