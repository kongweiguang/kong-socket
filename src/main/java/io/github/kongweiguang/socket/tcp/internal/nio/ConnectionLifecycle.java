package io.github.kongweiguang.socket.tcp.internal.nio;

/**
 * 连接关闭生命周期回调，用于运行时感知连接释放并触发后续清理。
 *
 * @author kongweiguang
 */
interface ConnectionLifecycle {

    /**
     * 连接关闭后触发。
     *
     * @param connection 已关闭的连接
     * @param cause 关闭原因，主动关闭时可能为 null
     */
    void onClosed(NioConnection connection, Throwable cause);
}
