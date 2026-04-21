package io.github.kongweiguang.socket.tcp.internal.aio;

import io.github.kongweiguang.socket.tcp.Connection;
import io.github.kongweiguang.socket.tcp.TcpHandler;
import io.github.kongweiguang.socket.tcp.codec.CodecSession;
import io.github.kongweiguang.socket.tcp.exception.ConnectionClosedException;
import io.github.kongweiguang.socket.tcp.internal.PendingWrite;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 基于 {@link AsynchronousSocketChannel} 的 TCP 连接实现。
 *
 * @author kongweiguang
 */
public final class AioConnection implements Connection {

    private final AsynchronousSocketChannel channel;
    private final CodecSession codecSession;
    private final TcpHandler handler;
    private final AioConnectionLifecycle lifecycle;
    private final int bufferSize;
    private final int writeQueueSize;
    private final Queue<PendingWrite> writes = new ArrayDeque<>();
    private final AtomicBoolean open = new AtomicBoolean(true);
    private boolean writing;

    AioConnection(AsynchronousSocketChannel channel, CodecSession codecSession, TcpHandler handler,
                  AioConnectionLifecycle lifecycle, int bufferSize, int writeQueueSize) {
        this.channel = channel;
        this.codecSession = codecSession;
        this.handler = handler;
        this.lifecycle = lifecycle;
        this.bufferSize = bufferSize;
        this.writeQueueSize = writeQueueSize;
    }

    /**
     * 触发连接建立回调。
     */
    void fireConnect() {
        try {
            handler.onConnect(this);
        } catch (RuntimeException e) {
            fireError(e);
            close(e);
        }
    }

    /**
     * 启动异步读循环。
     */
    void beginRead() {
        if (!isOpen()) {
            return;
        }
        ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
        channel.read(buffer, buffer, new CompletionHandler<>() {
            @Override
            public void completed(Integer read, ByteBuffer attachment) {
                if (read == null || read < 0) {
                    close(new ConnectionClosedException("remote closed connection"));
                    return;
                }
                if (read > 0) {
                    attachment.flip();
                    try {
                        List<ByteBuffer> messages = codecSession.decode(attachment);
                        for (ByteBuffer message : messages) {
                            handler.onMessage(AioConnection.this, message.asReadOnlyBuffer());
                        }
                    } catch (RuntimeException e) {
                        fireError(e);
                        close(e);
                        return;
                    }
                }
                beginRead();
            }

            @Override
            public void failed(Throwable error, ByteBuffer attachment) {
                fireError(error);
                close(error);
            }
        });
    }

    @Override
    public SocketAddress localAddress() {
        try {
            return channel.getLocalAddress();
        } catch (IOException e) {
            throw new ConnectionClosedException("local address unavailable: " + e.getMessage());
        }
    }

    @Override
    public SocketAddress remoteAddress() {
        try {
            return channel.getRemoteAddress();
        } catch (IOException e) {
            throw new ConnectionClosedException("remote address unavailable: " + e.getMessage());
        }
    }

    @Override
    public boolean isOpen() {
        return open.get() && channel.isOpen();
    }

    @Override
    public CompletableFuture<Void> send(ByteBuffer message) {
        if (!isOpen()) {
            return CompletableFuture.failedFuture(new ConnectionClosedException("connection is closed"));
        }

        CompletableFuture<Void> future = new CompletableFuture<>();
        ByteBuffer encoded;
        try {
            encoded = codecSession.encode(message.asReadOnlyBuffer());
        } catch (RuntimeException e) {
            future.completeExceptionally(e);
            return future;
        }

        synchronized (writes) {
            if (writes.size() >= writeQueueSize) {
                future.completeExceptionally(new IllegalStateException("write queue is full"));
                return future;
            }
            writes.offer(new PendingWrite(encoded, future));
        }
        drainWrites();
        return future;
    }

    @Override
    public void close() {
        close(null);
    }

    void close(Throwable cause) {
        if (!open.compareAndSet(true, false)) {
            return;
        }
        try {
            channel.close();
        } catch (IOException ignored) {
        }
        fail(new ConnectionClosedException("connection is closed"));
        try {
            handler.onClose(this);
        } catch (RuntimeException e) {
            fireError(e);
        }
        lifecycle.onClosed(this, cause);
    }

    private void drainWrites() {
        PendingWrite pending;
        synchronized (writes) {
            if (writing || !isOpen()) {
                return;
            }
            pending = writes.peek();
            if (pending == null) {
                return;
            }
            writing = true;
        }
        writeCurrent(pending);
    }

    private void writeCurrent(PendingWrite pending) {
        channel.write(pending.buffer(), pending, new CompletionHandler<>() {
            @Override
            public void completed(Integer written, PendingWrite attachment) {
                if (!isOpen()) {
                    fail(new ConnectionClosedException("connection is closed"));
                    return;
                }
                if (attachment.buffer().hasRemaining()) {
                    writeCurrent(attachment);
                    return;
                }
                synchronized (writes) {
                    writes.poll();
                    writing = false;
                }
                attachment.future().complete(null);
                drainWrites();
            }

            @Override
            public void failed(Throwable error, PendingWrite attachment) {
                fail(error);
                fireError(error);
                close(error);
            }
        });
    }

    private void fail(Throwable error) {
        synchronized (writes) {
            PendingWrite pending;
            while ((pending = writes.poll()) != null) {
                pending.future().completeExceptionally(error);
            }
            writing = false;
        }
    }

    private void fireError(Throwable error) {
        try {
            handler.onError(this, error);
        } catch (RuntimeException ignored) {
        }
    }
}
