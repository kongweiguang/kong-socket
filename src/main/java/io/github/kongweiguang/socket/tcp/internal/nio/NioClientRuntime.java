package io.github.kongweiguang.socket.tcp.internal.nio;

import io.github.kongweiguang.socket.tcp.Connection;
import io.github.kongweiguang.socket.tcp.KongTcpClientConfig;
import io.github.kongweiguang.socket.tcp.ReconnectConfig;
import io.github.kongweiguang.socket.tcp.exception.KongSocketException;
import io.github.kongweiguang.socket.tcp.internal.ClientRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * TCP 客户端运行时，负责非阻塞连接建立、读写事件循环和重连调度。
 *
 * @author kongweiguang
 */
public final class NioClientRuntime implements ClientRuntime {

    private static final Logger log = LoggerFactory.getLogger(ClientRuntime.class);

    private final KongTcpClientConfig config;
    private final ClientLoop loop = new ClientLoop();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(runnable ->
            Thread.ofPlatform().name("kong-socket-client-reconnect").daemon(true).unstarted(runnable)
    );
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicInteger attempts = new AtomicInteger();
    private CompletableFuture<Connection> firstConnectFuture;
    private volatile NioConnection connection;

    /**
     * 创建客户端运行时。
     *
     * @param config 客户端配置
     */
    public NioClientRuntime(KongTcpClientConfig config) {
        this.config = config;
    }

    /**
     * 发起连接并返回首次连接结果。
     *
     * @return 首次连接结果
     */
    public CompletableFuture<Connection> connect() {
        if (closed.get()) {
            return CompletableFuture.failedFuture(new KongSocketException("client is closed"));
        }
        if (!loop.isRunning()) {
            loop.start();
        }
        firstConnectFuture = new CompletableFuture<>();
        openChannel();
        return firstConnectFuture;
    }

    /**
     * 获取当前活跃连接。
     *
     * @return 当前连接，未连接时为 null
     */
    public Connection connection() {
        return connection;
    }

    /**
     * 打开非阻塞通道并发起一次连接尝试。
     */
    private void openChannel() {
        int attempt = attempts.incrementAndGet();
        InetSocketAddress remote = config.remoteAddress();
        config.listener().onConnecting(remote, attempt);
        loop.execute(() -> {
            try {
                SocketChannel channel = SocketChannel.open();
                channel.configureBlocking(false);
                boolean connected = channel.connect(remote);
                if (connected) {
                    finishConnect(channel, null);
                    return;
                }
                channel.register(loop.selector(), SelectionKey.OP_CONNECT, channel);
            } catch (IOException e) {
                handleConnectFailure(attempt, e);
            }
        });
    }

    /**
     * 完成非阻塞连接，创建连接对象并注册读事件。
     *
     * @param channel 已连接或正在完成连接的通道
     * @param key 连接阶段的 selection key，立即连接成功时为 null
     */
    private void finishConnect(SocketChannel channel, SelectionKey key) {
        try {
            if (!channel.finishConnect()) {
                return;
            }
            NioConnection newConnection = new NioConnection(
                    channel,
                    loop,
                    config.codec().newSession(),
                    config.handler(),
                    loop,
                    config.bufferSize(),
                    config.writeQueueSize()
            );
            SelectionKey readKey = key == null
                    ? channel.register(loop.selector(), SelectionKey.OP_READ, newConnection)
                    : key;
            readKey.attach(newConnection);
            readKey.interestOps(SelectionKey.OP_READ);
            newConnection.selectionKey(readKey);
            connection = newConnection;
            attempts.set(0);
            config.listener().onConnected(newConnection);
            newConnection.fireConnect();
            if (firstConnectFuture != null && !firstConnectFuture.isDone()) {
                firstConnectFuture.complete(newConnection);
            }
        } catch (IOException e) {
            try {
                channel.close();
            } catch (IOException ignored) {
            }
            handleConnectFailure(attempts.get(), e);
        }
    }

    /**
     * 处理连接失败事件，并按配置决定是否继续重连。
     *
     * @param attempt 当前尝试次数
     * @param error 失败异常
     */
    private void handleConnectFailure(int attempt, Throwable error) {
        config.listener().onConnectFailed(config.remoteAddress(), attempt, error);
        if (firstConnectFuture != null && !firstConnectFuture.isDone()) {
            firstConnectFuture.completeExceptionally(error);
        }
        scheduleReconnect(attempt, error);
    }

    /**
     * 根据重连策略调度下一次连接尝试。
     *
     * @param attempt 当前尝试次数
     * @param error 触发重连的异常
     */
    private void scheduleReconnect(int attempt, Throwable error) {
        if (closed.get()) {
            return;
        }
        ReconnectConfig reconnect = config.reconnectConfig();
        if (!reconnect.enabled() || attempt > reconnect.maxAttempts()) {
            config.listener().onReconnectFailed(config.remoteAddress(), attempt, error);
            return;
        }
        Duration delay = reconnect.delayFor(attempt);
        int nextAttempt = attempt + 1;
        config.listener().onReconnectScheduled(config.remoteAddress(), nextAttempt, delay);
        scheduler.schedule(this::openChannel, delay.toMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * 关闭客户端运行时、当前连接、事件循环和重连调度器。
     */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        NioConnection current = connection;
        if (current != null) {
            current.close();
        }
        loop.close();
        scheduler.shutdownNow();
    }

    private final class ClientLoop extends EventLoop implements ConnectionLifecycle {

        ClientLoop() {
            super("kong-socket-client");
        }

        @Override
        protected void handle(SelectionKey key) {
            try {
                if (key.isConnectable()) {
                    finishConnect((SocketChannel) key.channel(), key);
                    return;
                }
                NioConnection current = (NioConnection) key.attachment();
                if (key.isValid() && key.isReadable()) {
                    current.read();
                }
                if (key.isValid() && key.isWritable()) {
                    current.flush();
                }
            } catch (RuntimeException e) {
                onLoopError(e);
            }
        }

        @Override
        protected void onLoopError(Throwable error) {
            log.error("kong tcp client loop error", error);
            config.handler().onError(connection, error);
        }

        @Override
        public void onClosed(NioConnection closedConnection, Throwable cause) {
            if (connection == closedConnection) {
                connection = null;
            }
            config.listener().onDisconnected(closedConnection);
            if (!closed.get()) {
                int attempt = attempts.incrementAndGet();
                scheduleReconnect(attempt, cause == null ? new KongSocketException("connection closed") : cause);
            }
        }
    }
}
