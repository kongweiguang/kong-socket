package io.github.kongweiguang.socket.tcp.internal.nio;

import io.github.kongweiguang.socket.tcp.KongTcpServerConfig;
import io.github.kongweiguang.socket.tcp.exception.KongSocketException;
import io.github.kongweiguang.socket.tcp.internal.ServerRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * TCP 服务端运行时，负责端口绑定、boss/worker 事件循环和连接注册。
 *
 * @author kongweiguang
 */
public final class NioServerRuntime implements ServerRuntime {

    private static final Logger log = LoggerFactory.getLogger(ServerRuntime.class);

    private final KongTcpServerConfig config;
    private final BossLoop[] bossLoops;
    private final WorkerLoop[] workerLoops;
    private final AtomicInteger bossCounter = new AtomicInteger();
    private final AtomicInteger workerCounter = new AtomicInteger();
    private final List<ServerSocketChannel> serverChannels = new ArrayList<>();

    /**
     * 创建服务端运行时。
     *
     * @param config 服务端配置
     */
    public NioServerRuntime(KongTcpServerConfig config) {
        this.config = config;
        this.bossLoops = new BossLoop[config.bossThreads()];
        this.workerLoops = new WorkerLoop[config.workerThreads()];
        for (int i = 0; i < bossLoops.length; i++) {
            bossLoops[i] = new BossLoop("kong-socket-boss-" + i);
        }
        for (int i = 0; i < workerLoops.length; i++) {
            workerLoops[i] = new WorkerLoop("kong-socket-worker-" + i);
        }
    }

    /**
     * 启动 boss/worker 事件循环并绑定监听地址。
     */
    public void start() {
        for (WorkerLoop workerLoop : workerLoops) {
            workerLoop.start();
        }
        for (BossLoop bossLoop : bossLoops) {
            bossLoop.start();
        }
        for (InetSocketAddress address : config.bindAddresses()) {
            bind(address);
        }
    }

    /**
     * 获取服务端实际绑定地址。
     *
     * @return 实际绑定地址列表
     */
    public List<InetSocketAddress> boundAddresses() {
        List<InetSocketAddress> addresses = new ArrayList<>();
        for (ServerSocketChannel channel : serverChannels) {
            try {
                addresses.add((InetSocketAddress) channel.getLocalAddress());
            } catch (IOException e) {
                throw new KongSocketException("read server bind address failed", e);
            }
        }
        return List.copyOf(addresses);
    }

    /**
     * 绑定单个监听地址，并将 server channel 注册到 boss 事件循环。
     *
     * @param address 监听地址
     */
    private void bind(InetSocketAddress address) {
        try {
            ServerSocketChannel server = ServerSocketChannel.open();
            server.configureBlocking(false);
            server.bind(address);
            serverChannels.add(server);
            BossLoop bossLoop = nextBoss();
            bossLoop.execute(() -> {
                try {
                    server.register(bossLoop.selector(), SelectionKey.OP_ACCEPT);
                    log.info("kong tcp server listening on {}", localAddress(server));
                } catch (IOException e) {
                    throw new KongSocketException("register server channel failed", e);
                }
            });
        } catch (IOException e) {
            throw new KongSocketException("bind failed: " + address, e);
        }
    }

    /**
     * 轮询选择下一个 boss 事件循环。
     *
     * @return boss 事件循环
     */
    private BossLoop nextBoss() {
        int index = Math.floorMod(bossCounter.getAndIncrement(), bossLoops.length);
        return bossLoops[index];
    }

    /**
     * 轮询选择下一个 worker 事件循环。
     *
     * @return worker 事件循环
     */
    private WorkerLoop nextWorker() {
        int index = Math.floorMod(workerCounter.getAndIncrement(), workerLoops.length);
        return workerLoops[index];
    }

    /**
     * 读取 server channel 的本地地址，日志场景下失败时返回 unknown。
     *
     * @param server 服务端通道
     * @return 本地地址文本
     */
    private String localAddress(ServerSocketChannel server) {
        try {
            return String.valueOf(server.getLocalAddress());
        } catch (IOException e) {
            return "unknown";
        }
    }

    /**
     * 关闭所有监听通道和事件循环。
     */
    @Override
    public void close() {
        for (ServerSocketChannel serverChannel : serverChannels) {
            try {
                serverChannel.close();
            } catch (IOException ignored) {
            }
        }
        for (BossLoop bossLoop : bossLoops) {
            bossLoop.close();
        }
        for (WorkerLoop workerLoop : workerLoops) {
            workerLoop.close();
        }
    }

    private final class BossLoop extends EventLoop {

        BossLoop(String name) {
            super(name);
        }

        @Override
        protected void handle(SelectionKey key) throws IOException {
            if (!key.isAcceptable()) {
                return;
            }
            ServerSocketChannel server = (ServerSocketChannel) key.channel();
            SocketChannel client;
            while ((client = server.accept()) != null) {
                client.configureBlocking(false);
                nextWorker().register(client);
            }
        }

        @Override
        protected void onLoopError(Throwable error) {
            log.error("kong tcp boss loop error", error);
        }
    }

    private final class WorkerLoop extends EventLoop implements ConnectionLifecycle {

        WorkerLoop(String name) {
            super(name);
        }

        void register(SocketChannel channel) {
            execute(() -> {
                try {
                    NioConnection connection = new NioConnection(
                            channel,
                            this,
                            config.codec().newSession(),
                            config.handler(),
                            this,
                            config.bufferSize(),
                            config.writeQueueSize()
                    );
                    SelectionKey key = channel.register(selector(), SelectionKey.OP_READ, connection);
                    connection.selectionKey(key);
                    connection.fireConnect();
                } catch (IOException e) {
                    try {
                        channel.close();
                    } catch (IOException ignored) {
                    }
                    config.handler().onError(null, e);
                }
            });
        }

        @Override
        protected void handle(SelectionKey key) {
            NioConnection connection = (NioConnection) key.attachment();
            if (key.isValid() && key.isReadable()) {
                connection.read();
            }
            if (key.isValid() && key.isWritable()) {
                connection.flush();
            }
        }

        @Override
        protected void onLoopError(Throwable error) {
            log.error("kong tcp worker loop error", error);
        }

        @Override
        public void onClosed(NioConnection connection, Throwable cause) {
        }
    }
}
