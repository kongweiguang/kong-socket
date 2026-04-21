package io.github.kongweiguang.socket.tcp.internal;

import io.github.kongweiguang.socket.tcp.KongTcpClientConfig;
import io.github.kongweiguang.socket.tcp.KongTcpServerConfig;
import io.github.kongweiguang.socket.tcp.internal.aio.AioClientRuntime;
import io.github.kongweiguang.socket.tcp.internal.aio.AioServerRuntime;
import io.github.kongweiguang.socket.tcp.internal.nio.NioClientRuntime;
import io.github.kongweiguang.socket.tcp.internal.nio.NioServerRuntime;

/**
 * 内置 TCP 驱动运行时工厂。
 *
 * @author kongweiguang
 */
public final class DriverRuntimes {

    private DriverRuntimes() {
    }

    /**
     * 根据服务端配置创建运行时。
     *
     * @param config 服务端配置
     * @return 服务端运行时
     */
    public static ServerRuntime server(KongTcpServerConfig config) {
        return switch (config.driver()) {
            case NIO -> new NioServerRuntime(config);
            case AIO -> new AioServerRuntime(config);
        };
    }

    /**
     * 根据客户端配置创建运行时。
     *
     * @param config 客户端配置
     * @return 客户端运行时
     */
    public static ClientRuntime client(KongTcpClientConfig config) {
        return switch (config.driver()) {
            case NIO -> new NioClientRuntime(config);
            case AIO -> new AioClientRuntime(config);
        };
    }
}
