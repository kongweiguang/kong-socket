package io.github.kongweiguang.socket.tcp;

import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

/**
 * 表示一条处于生命周期中的 TCP 连接。
 *
 * @author kongweiguang
 */
public interface Connection extends AutoCloseable {

    /**
     * 获取本地 socket 地址。
     *
     * @return 本地地址
     */
    SocketAddress localAddress();

    /**
     * 获取远端 socket 地址。
     *
     * @return 远端地址
     */
    SocketAddress remoteAddress();

    /**
     * 判断连接是否仍然打开。
     *
     * @return 连接打开时返回 true
     */
    boolean isOpen();

    /**
     * 异步发送一条二进制消息。
     *
     * @param message 消息字节缓冲区
     * @return 发送完成结果
     */
    CompletableFuture<Void> send(ByteBuffer message);

    /**
     * 异步发送一条二进制消息。
     *
     * @param message 消息字节数组
     * @return 发送完成结果
     */
    default CompletableFuture<Void> send(byte[] message) {
        return send(ByteBuffer.wrap(message));
    }

    /**
     * 使用 UTF-8 编码异步发送文本消息。
     *
     * @param message 消息文本
     * @return 发送完成结果
     */
    default CompletableFuture<Void> send(String message) {
        return send(message, StandardCharsets.UTF_8);
    }

    /**
     * 使用指定字符集异步发送文本消息。
     *
     * @param message 消息文本
     * @param charset 字符集
     * @return 发送完成结果
     */
    default CompletableFuture<Void> send(String message, Charset charset) {
        return send(ByteBuffer.wrap(message.getBytes(charset)));
    }

    @Override
    void close();
}
