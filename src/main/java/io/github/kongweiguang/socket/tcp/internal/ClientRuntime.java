package io.github.kongweiguang.socket.tcp.internal;

import io.github.kongweiguang.socket.tcp.Connection;

import java.util.concurrent.CompletableFuture;

/**
 * TCP 客户端运行时内部契约。
 *
 * @author kongweiguang
 */
public interface ClientRuntime extends AutoCloseable {

    /**
     * 发起连接并返回首次连接结果。
     *
     * @return 首次连接结果
     */
    CompletableFuture<Connection> connect();

    /**
     * 获取当前活跃连接。
     *
     * @return 当前连接，未连接时返回 null
     */
    Connection connection();

    /**
     * 关闭运行时并释放资源。
     */
    @Override
    void close();
}
