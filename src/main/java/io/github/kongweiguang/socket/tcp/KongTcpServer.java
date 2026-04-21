package io.github.kongweiguang.socket.tcp;

import io.github.kongweiguang.socket.tcp.exception.KongSocketException;
import io.github.kongweiguang.socket.tcp.internal.DriverRuntimes;
import io.github.kongweiguang.socket.tcp.internal.ServerRuntime;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 支持可选 IO 驱动的轻量级 TCP 服务端。
 *
 * @author kongweiguang
 */
public final class KongTcpServer implements AutoCloseable {

    private final KongTcpServerConfig config;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private ServerRuntime runtime;

    KongTcpServer(KongTcpServerConfig config) {
        this.config = config;
    }

    /**
     * 创建服务端配置构建器。
     *
     * @return 服务端配置构建器
     */
    public static KongTcpServerConfig.Builder builder() {
        return new KongTcpServerConfig.Builder();
    }

    /**
     * 启动服务端。
     *
     * @return 当前服务端实例
     */
    public KongTcpServer start() {
        if (!running.compareAndSet(false, true)) {
            return this;
        }
        try {
            runtime = DriverRuntimes.server(config);
            runtime.start();
            return this;
        } catch (RuntimeException e) {
            running.set(false);
            throw e;
        }
    }

    /**
     * 判断服务端是否正在运行。
     *
     * @return 运行中返回 true
     */
    public boolean isRunning() {
        return running.get();
    }

    /**
     * 获取实际监听地址，测试场景使用端口 0 绑定时可用于读取真实端口。
     *
     * @return 实际监听地址列表
     */
    public List<InetSocketAddress> boundAddresses() {
        if (!isRunning() || runtime == null) {
            throw new KongSocketException("server is not running");
        }
        return runtime.boundAddresses();
    }

    /**
     * 获取服务端不可变配置。
     *
     * @return 服务端配置
     */
    public KongTcpServerConfig config() {
        return config;
    }

    /**
     * 关闭服务端并释放监听通道和事件循环资源。
     */
    @Override
    public void close() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        if (runtime != null) {
            runtime.close();
        }
    }
}
