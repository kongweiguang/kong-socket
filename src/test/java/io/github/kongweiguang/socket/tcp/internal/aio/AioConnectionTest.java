package io.github.kongweiguang.socket.tcp.internal.aio;

import io.github.kongweiguang.socket.tcp.codec.Codecs;
import io.github.kongweiguang.socket.tcp.internal.aio.AioConnection;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.SocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.ShutdownChannelGroupException;
import java.nio.channels.UnsupportedAddressTypeException;
import java.nio.channels.spi.AsynchronousChannelProvider;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * AIO 连接写队列边界测试。
 *
 * @author kongweiguang
 */
class AioConnectionTest {

    @Test
    void writeQueueLimitRejectsNewSendWhenPreviousWriteIsPending() {
        FakeSocketChannel channel = new FakeSocketChannel();
        AioConnection connection = newConnection(channel, 1);

        CompletableFuture<Void> first = connection.send(ByteBuffer.wrap(new byte[]{1}));
        CompletableFuture<Void> second = connection.send(ByteBuffer.wrap(new byte[]{2}));

        assertFalse(first.isDone());
        assertTrue(second.isCompletedExceptionally());
        assertEquals(1, channel.writes.get());
    }

    @Test
    void closeFailsPendingSendFuture() {
        FakeSocketChannel channel = new FakeSocketChannel();
        AioConnection connection = newConnection(channel, 8);

        CompletableFuture<Void> pending = connection.send(ByteBuffer.wrap(new byte[]{1}));
        connection.close();

        assertTrue(pending.isCompletedExceptionally());
        assertFalse(connection.isOpen());
    }

    private AioConnection newConnection(FakeSocketChannel channel, int writeQueueSize) {
        return new AioConnection(
                channel,
                Codecs.raw().newSession(),
                (connection, message) -> {
                },
                (connection, cause) -> {
                },
                1024,
                writeQueueSize
        );
    }

    private static final class FakeSocketChannel extends AsynchronousSocketChannel {

        private final AtomicInteger writes = new AtomicInteger();
        private volatile boolean open = true;

        private FakeSocketChannel() {
            super(AsynchronousChannelProvider.provider());
        }

        @Override
        public AsynchronousSocketChannel bind(SocketAddress local) {
            return this;
        }

        @Override
        public <T> AsynchronousSocketChannel setOption(java.net.SocketOption<T> name, T value) {
            return this;
        }

        @Override
        public AsynchronousSocketChannel shutdownInput() {
            return this;
        }

        @Override
        public AsynchronousSocketChannel shutdownOutput() {
            return this;
        }

        @Override
        public SocketAddress getRemoteAddress() {
            return null;
        }

        @Override
        public <A> void connect(SocketAddress remote, A attachment, CompletionHandler<Void, ? super A> handler) {
            handler.failed(new UnsupportedAddressTypeException(), attachment);
        }

        @Override
        public Future<Void> connect(SocketAddress remote) {
            return CompletableFuture.failedFuture(new ShutdownChannelGroupException());
        }

        @Override
        public <A> void read(ByteBuffer dst, long timeout, TimeUnit unit, A attachment,
                             CompletionHandler<Integer, ? super A> handler) {
            handler.failed(new IOException("read unsupported"), attachment);
        }

        @Override
        public Future<Integer> read(ByteBuffer dst) {
            return CompletableFuture.failedFuture(new IOException("read unsupported"));
        }

        @Override
        public <A> void write(ByteBuffer src, long timeout, TimeUnit unit, A attachment,
                              CompletionHandler<Integer, ? super A> handler) {
            writes.incrementAndGet();
        }

        @Override
        public Future<Integer> write(ByteBuffer src) {
            writes.incrementAndGet();
            return new CompletableFuture<>();
        }

        @Override
        public <A> void read(ByteBuffer[] dsts, int offset, int length, long timeout, TimeUnit unit, A attachment,
                             CompletionHandler<Long, ? super A> handler) {
            handler.failed(new IOException("read unsupported"), attachment);
        }

        @Override
        public <A> void write(ByteBuffer[] srcs, int offset, int length, long timeout, TimeUnit unit, A attachment,
                              CompletionHandler<Long, ? super A> handler) {
            writes.incrementAndGet();
        }

        @Override
        public SocketAddress getLocalAddress() {
            return null;
        }

        @Override
        public <T> T getOption(java.net.SocketOption<T> name) {
            return null;
        }

        @Override
        public Set<java.net.SocketOption<?>> supportedOptions() {
            return Set.of(StandardSocketOptions.SO_KEEPALIVE);
        }

        @Override
        public boolean isOpen() {
            return open;
        }

        @Override
        public void close() {
            open = false;
        }
    }
}
