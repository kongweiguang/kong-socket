package io.github.kongweiguang.socket.tcp;

import java.nio.ByteBuffer;

/**
 * 处理连接生命周期事件和已经完成解码的消息。
 *
 * @author kongweiguang
 */
@FunctionalInterface
public interface TcpHandler {

    /**
     * 连接注册到事件循环后触发。
     *
     * @param connection 当前连接
     */
    default void onConnect(Connection connection) {
    }

    /**
     * 每收到一条完整解码消息时触发。
     *
     * @param connection 当前连接
     * @param message 解码后的消息
     */
    void onMessage(Connection connection, ByteBuffer message);

    /**
     * 连接或业务处理器报告异常时触发。
     *
     * @param connection 当前连接，连接失败时可能为 null
     * @param error 异常信息
     */
    default void onError(Connection connection, Throwable error) {
    }

    /**
     * 连接关闭时触发。
     *
     * @param connection 当前连接
     */
    default void onClose(Connection connection) {
    }
}
