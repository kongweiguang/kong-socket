package io.github.kongweiguang.socket.tcp.internal;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * TCP 服务端运行时内部契约。
 *
 * @author kongweiguang
 */
public interface ServerRuntime extends AutoCloseable {

    /**
     * 启动服务端运行时。
     */
    void start();

    /**
     * 获取实际监听地址。
     *
     * @return 实际监听地址列表
     */
    List<InetSocketAddress> boundAddresses();

    /**
     * 关闭运行时并释放资源。
     */
    @Override
    void close();
}
