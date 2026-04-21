package io.github.kongweiguang.socket.tcp.internal.nio;

import io.github.kongweiguang.socket.tcp.Connection;
import io.github.kongweiguang.socket.tcp.TcpHandler;
import io.github.kongweiguang.socket.tcp.codec.CodecSession;
import io.github.kongweiguang.socket.tcp.exception.CodecException;
import io.github.kongweiguang.socket.tcp.exception.ConnectionClosedException;
import io.github.kongweiguang.socket.tcp.internal.PendingWrite;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 基于非阻塞 SocketChannel 的连接实现，负责读写、编解码和连接生命周期派发。
 *
 * @author kongweiguang
 */
public final class NioConnection implements Connection {

    private final SocketChannel channel;
    private final EventLoop eventLoop;
    private final CodecSession codecSession;
    private final TcpHandler handler;
    private final ConnectionLifecycle lifecycle;
    private final int bufferSize;
    private final int writeQueueSize;
    private final Queue<PendingWrite> writes = new ArrayDeque<>();
    private final AtomicBoolean open = new AtomicBoolean(true);
    private SelectionKey key;

    NioConnection(SocketChannel channel, EventLoop eventLoop, CodecSession codecSession, TcpHandler handler,
                  ConnectionLifecycle lifecycle, int bufferSize, int writeQueueSize) {
        this.channel = channel;
        this.eventLoop = eventLoop;
        this.codecSession = codecSession;
        this.handler = handler;
        this.lifecycle = lifecycle;
        this.bufferSize = bufferSize;
        this.writeQueueSize = writeQueueSize;
    }

    /**
     * 绑定当前连接注册到 selector 后得到的 selection key。
     *
     * @param key selection key
     */
    void selectionKey(SelectionKey key) {
        this.key = key;
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
     * 从通道读取数据，交给编解码会话切帧后逐条派发给处理器。
     */
    void read() {
        ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
        try {
            int read = channel.read(buffer);
            if (read < 0) {
                close(new ConnectionClosedException("remote closed connection"));
                return;
            }
            if (read == 0) {
                return;
            }
            buffer.flip();
            List<ByteBuffer> messages = codecSession.decode(buffer);
            for (ByteBuffer message : messages) {
                handler.onMessage(this, message.asReadOnlyBuffer());
            }
        } catch (CodecException e) {
            fireError(e);
            close(e);
        } catch (IOException e) {
            fireError(e);
            close(e);
        } catch (RuntimeException e) {
            fireError(e);
            close(e);
        }
    }

    /**
     * 尽可能刷写待写队列，遇到 partial write 时保留 OP_WRITE 监听。
     */
    void flush() {
        try {
            while (true) {
                PendingWrite pending;
                synchronized (writes) {
                    pending = writes.peek();
                }
                if (pending == null) {
                    disableWriteInterest();
                    return;
                }

                channel.write(pending.buffer());
                if (pending.buffer().hasRemaining()) {
                    enableWriteInterest();
                    return;
                }

                synchronized (writes) {
                    writes.poll();
                }
                pending.future().complete(null);
            }
        } catch (IOException e) {
            fail(e);
            fireError(e);
            close(e);
        }
    }

    /**
     * 获取本地 socket 地址。
     *
     * @return 本地地址
     */
    @Override
    public SocketAddress localAddress() {
        try {
            return channel.getLocalAddress();
        } catch (IOException e) {
            throw new ConnectionClosedException("local address unavailable: " + e.getMessage());
        }
    }

    /**
     * 获取远端 socket 地址。
     *
     * @return 远端地址
     */
    @Override
    public SocketAddress remoteAddress() {
        try {
            return channel.getRemoteAddress();
        } catch (IOException e) {
            throw new ConnectionClosedException("remote address unavailable: " + e.getMessage());
        }
    }

    /**
     * 判断底层通道和连接状态是否仍然打开。
     *
     * @return 连接打开时返回 true
     */
    @Override
    public boolean isOpen() {
        return open.get() && channel.isOpen();
    }

    /**
     * 编码并异步发送一条消息。
     *
     * @param message 待发送消息
     * @return 发送完成结果
     */
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

        eventLoop.execute(() -> {
            if (!isOpen()) {
                fail(new ConnectionClosedException("connection is closed"));
                return;
            }
            enableWriteInterest();
            flush();
        });
        return future;
    }

    /**
     * 主动关闭连接。
     */
    @Override
    public void close() {
        close(null);
    }

    /**
     * 使用指定原因关闭连接。
     *
     * @param cause 关闭原因，主动关闭时可能为 null
     */
    void close(Throwable cause) {
        if (!open.compareAndSet(true, false)) {
            return;
        }
        Runnable closeTask = () -> doClose(cause);
        if (eventLoop.inEventLoop()) {
            closeTask.run();
        } else {
            eventLoop.execute(closeTask);
        }
    }

    private void doClose(Throwable cause) {
        if (key != null) {
            key.cancel();
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

    private void enableWriteInterest() {
        if (key != null && key.isValid()) {
            key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
        }
    }

    private void disableWriteInterest() {
        if (key != null && key.isValid()) {
            key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
        }
    }

    private void fail(Throwable error) {
        synchronized (writes) {
            PendingWrite pending;
            while ((pending = writes.poll()) != null) {
                pending.future().completeExceptionally(error);
            }
        }
    }

    private void fireError(Throwable error) {
        try {
            handler.onError(this, error);
        } catch (RuntimeException ignored) {
        }
    }
}
