package io.github.kongweiguang.socket.tcp.internal.aio;

import io.github.kongweiguang.socket.tcp.KongTcpServerConfig;
import io.github.kongweiguang.socket.tcp.exception.KongSocketException;
import io.github.kongweiguang.socket.tcp.internal.ServerRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 基于 Java AIO 的 TCP 服务端运行时。
 *
 * @author kongweiguang
 */
public final class AioServerRuntime implements ServerRuntime {

    private static final Logger log = LoggerFactory.getLogger(AioServerRuntime.class);

    private final KongTcpServerConfig config;
    private final List<AsynchronousServerSocketChannel> serverChannels = new ArrayList<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private AsynchronousChannelGroup group;

    /**
     * 创建 AIO 服务端运行时。
     *
     * @param config 服务端配置
     */
    public AioServerRuntime(KongTcpServerConfig config) {
        this.config = config;
    }

    @Override
    public void start() {
        try {
            group = AsynchronousChannelGroup.withFixedThreadPool(
                    config.workerThreads(),
                    threadFactory("kong-socket-aio-server-")
            );
            for (InetSocketAddress address : config.bindAddresses()) {
                bind(address);
            }
        } catch (IOException e) {
            throw new KongSocketException("start aio server failed", e);
        }
    }

    @Override
    public List<InetSocketAddress> boundAddresses() {
        List<InetSocketAddress> addresses = new ArrayList<>();
        for (AsynchronousServerSocketChannel channel : serverChannels) {
            try {
                addresses.add((InetSocketAddress) channel.getLocalAddress());
            } catch (IOException e) {
                throw new KongSocketException("read server bind address failed", e);
            }
        }
        return List.copyOf(addresses);
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        for (AsynchronousServerSocketChannel channel : serverChannels) {
            try {
                channel.close();
            } catch (IOException ignored) {
            }
        }
        if (group != null) {
            try {
                group.shutdownNow();
            } catch (IOException ignored) {
            }
        }
    }

    private void bind(InetSocketAddress address) throws IOException {
        AsynchronousServerSocketChannel server = AsynchronousServerSocketChannel.open(group);
        server.bind(address);
        serverChannels.add(server);
        log.info("kong tcp aio server listening on {}", server.getLocalAddress());
        acceptLoop(server);
    }

    private void acceptLoop(AsynchronousServerSocketChannel server) {
        if (closed.get() || !server.isOpen()) {
            return;
        }
        server.accept(null, new CompletionHandler<>() {
            @Override
            public void completed(AsynchronousSocketChannel channel, Object attachment) {
                acceptLoop(server);
                AioConnection connection = new AioConnection(
                        channel,
                        config.codec().newSession(),
                        config.handler(),
                        (closedConnection, cause) -> {
                        },
                        config.bufferSize(),
                        config.writeQueueSize()
                );
                connection.fireConnect();
                connection.beginRead();
            }

            @Override
            public void failed(Throwable error, Object attachment) {
                if (closed.get()) {
                    return;
                }
                log.error("kong tcp aio accept failed", error);
                config.handler().onError(null, error);
                acceptLoop(server);
            }
        });
    }

    private ThreadFactory threadFactory(String prefix) {
        AtomicInteger counter = new AtomicInteger();
        return runnable -> Thread.ofPlatform()
                .name(prefix + counter.getAndIncrement())
                .daemon(true)
                .unstarted(runnable);
    }
}
