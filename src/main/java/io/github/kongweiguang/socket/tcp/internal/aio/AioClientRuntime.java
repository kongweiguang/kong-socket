package io.github.kongweiguang.socket.tcp.internal.aio;

import io.github.kongweiguang.socket.tcp.Connection;
import io.github.kongweiguang.socket.tcp.KongTcpClientConfig;
import io.github.kongweiguang.socket.tcp.ReconnectConfig;
import io.github.kongweiguang.socket.tcp.exception.KongSocketException;
import io.github.kongweiguang.socket.tcp.internal.ClientRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 基于 Java AIO 的 TCP 客户端运行时。
 *
 * @author kongweiguang
 */
public final class AioClientRuntime implements ClientRuntime {

    private static final Logger log = LoggerFactory.getLogger(AioClientRuntime.class);

    private final KongTcpClientConfig config;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(runnable ->
            Thread.ofPlatform().name("kong-socket-aio-client-reconnect").daemon(true).unstarted(runnable)
    );
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicInteger attempts = new AtomicInteger();
    private final AsynchronousChannelGroup group;
    private CompletableFuture<Connection> firstConnectFuture;
    private volatile AioConnection connection;

    /**
     * 创建 AIO 客户端运行时。
     *
     * @param config 客户端配置
     */
    public AioClientRuntime(KongTcpClientConfig config) {
        this.config = config;
        try {
            this.group = AsynchronousChannelGroup.withFixedThreadPool(1, runnable ->
                    Thread.ofPlatform().name("kong-socket-aio-client").daemon(true).unstarted(runnable)
            );
        } catch (IOException e) {
            throw new KongSocketException("open aio client group failed", e);
        }
    }

    @Override
    public CompletableFuture<Connection> connect() {
        if (closed.get()) {
            return CompletableFuture.failedFuture(new KongSocketException("client is closed"));
        }
        firstConnectFuture = new CompletableFuture<>();
        openChannel();
        return firstConnectFuture;
    }

    @Override
    public Connection connection() {
        return connection;
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        AioConnection current = connection;
        if (current != null) {
            current.close();
        }
        scheduler.shutdownNow();
        try {
            group.shutdownNow();
        } catch (IOException ignored) {
        }
    }

    private void openChannel() {
        int attempt = attempts.incrementAndGet();
        InetSocketAddress remote = config.remoteAddress();
        config.listener().onConnecting(remote, attempt);
        try {
            AsynchronousSocketChannel channel = AsynchronousSocketChannel.open(group);
            AtomicBoolean completed = new AtomicBoolean(false);
            ScheduledFuture<?> timeout = scheduler.schedule(() -> {
                if (!completed.compareAndSet(false, true)) {
                    return;
                }
                try {
                    channel.close();
                } catch (IOException ignored) {
                }
                handleConnectFailure(attempt, new KongSocketException("connect timeout: " + remote));
            }, 1, TimeUnit.SECONDS);
            channel.connect(remote, channel, new CompletionHandler<>() {
                @Override
                public void completed(Void result, AsynchronousSocketChannel attachment) {
                    if (!completed.compareAndSet(false, true)) {
                        return;
                    }
                    timeout.cancel(false);
                    finishConnect(attachment);
                }

                @Override
                public void failed(Throwable error, AsynchronousSocketChannel attachment) {
                    if (!completed.compareAndSet(false, true)) {
                        return;
                    }
                    timeout.cancel(false);
                    try {
                        attachment.close();
                    } catch (IOException ignored) {
                    }
                    handleConnectFailure(attempt, error);
                }
            });
        } catch (IOException e) {
            handleConnectFailure(attempt, e);
        }
    }

    private void finishConnect(AsynchronousSocketChannel channel) {
        AioConnection newConnection = new AioConnection(
                channel,
                config.codec().newSession(),
                config.handler(),
                this::onClosed,
                config.bufferSize(),
                config.writeQueueSize()
        );
        connection = newConnection;
        attempts.set(0);
        config.listener().onConnected(newConnection);
        newConnection.fireConnect();
        newConnection.beginRead();
        if (firstConnectFuture != null && !firstConnectFuture.isDone()) {
            firstConnectFuture.complete(newConnection);
        }
    }

    private void handleConnectFailure(int attempt, Throwable error) {
        config.listener().onConnectFailed(config.remoteAddress(), attempt, error);
        if (firstConnectFuture != null && !firstConnectFuture.isDone()) {
            firstConnectFuture.completeExceptionally(error);
        }
        scheduleReconnect(attempt, error);
    }

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

    private void onClosed(AioConnection closedConnection, Throwable cause) {
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
