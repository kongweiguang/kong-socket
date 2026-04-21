package io.github.kongweiguang.socket.tcp;

import io.github.kongweiguang.socket.tcp.internal.ClientRuntime;
import io.github.kongweiguang.socket.tcp.internal.DriverRuntimes;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 支持可选 IO 驱动的轻量级 TCP 客户端。
 *
 * @author kongweiguang
 */
public final class KongTcpClient implements AutoCloseable {

    private final KongTcpClientConfig config;
    private final AtomicBoolean connected = new AtomicBoolean(false);
    private ClientRuntime runtime;

    KongTcpClient(KongTcpClientConfig config) {
        this.config = config;
    }

    /**
     * 创建客户端配置构建器。
     *
     * @return 客户端配置构建器
     */
    public static KongTcpClientConfig.Builder builder() {
        return new KongTcpClientConfig.Builder();
    }

    /**
     * 连接到配置的远端地址。
     *
     * @return 连接成功后完成的连接结果
     */
    public CompletableFuture<Connection> connect() {
        if (connected.compareAndSet(false, true)) {
            runtime = DriverRuntimes.client(config);
            return runtime.connect();
        }
        Connection connection = runtime == null ? null : runtime.connection();
        if (connection != null && connection.isOpen()) {
            return CompletableFuture.completedFuture(connection);
        }
        return runtime.connect();
    }

    /**
     * 获取当前连接，未连接时返回 null。
     *
     * @return 当前连接
     */
    public Connection connection() {
        return runtime == null ? null : runtime.connection();
    }

    /**
     * 获取客户端不可变配置。
     *
     * @return 客户端配置
     */
    public KongTcpClientConfig config() {
        return config;
    }

    /**
     * 关闭客户端并释放运行时资源。
     */
    @Override
    public void close() {
        connected.set(false);
        if (runtime != null) {
            runtime.close();
        }
    }
}
