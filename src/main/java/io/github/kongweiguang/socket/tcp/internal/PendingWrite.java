package io.github.kongweiguang.socket.tcp.internal;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

/**
 * 单条待写出站数据及其异步发送结果。
 *
 * @author kongweiguang
 */
public final class PendingWrite {

    private final ByteBuffer buffer;
    private final CompletableFuture<Void> future;

    public PendingWrite(ByteBuffer buffer, CompletableFuture<Void> future) {
        this.buffer = buffer;
        this.future = future;
    }

    /**
     * 获取尚未完全写出的缓冲区。
     *
     * @return 待写缓冲区
     */
    public ByteBuffer buffer() {
        return buffer;
    }

    /**
     * 获取本次发送对应的完成结果。
     *
     * @return 发送完成结果
     */
    public CompletableFuture<Void> future() {
        return future;
    }
}
