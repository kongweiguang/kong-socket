package io.github.kongweiguang.socket.tcp;

/**
 * TCP 底层 IO 驱动类型。
 *
 * @author kongweiguang
 */
public enum TcpDriver {

    /**
     * 基于 {@code Selector} 和 {@code SocketChannel} 的 NIO 驱动。
     */
    NIO,

    /**
     * 基于 {@code AsynchronousSocketChannel} 的 AIO 驱动。
     */
    AIO
}
