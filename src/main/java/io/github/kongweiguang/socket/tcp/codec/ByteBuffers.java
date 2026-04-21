package io.github.kongweiguang.socket.tcp.codec;

import java.nio.ByteBuffer;

/**
 * ByteBuffer 复制和读取工具。
 *
 * @author kongweiguang
 */
final class ByteBuffers {

    private ByteBuffers() {
    }

    /**
     * 复制缓冲区剩余可读内容，不改变原缓冲区的 position。
     *
     * @param source 源缓冲区
     * @return 复制后的缓冲区
     */
    static ByteBuffer copy(ByteBuffer source) {
        ByteBuffer duplicate = source.slice();
        ByteBuffer copy = ByteBuffer.allocate(duplicate.remaining());
        copy.put(duplicate);
        copy.flip();
        return copy;
    }

    /**
     * 读取缓冲区剩余内容并推进原缓冲区的 position。
     *
     * @param source 源缓冲区
     * @return 读取到的字节数组
     */
    static byte[] readBytes(ByteBuffer source) {
        byte[] bytes = new byte[source.remaining()];
        source.get(bytes);
        return bytes;
    }

    /**
     * 将字节数组指定片段复制为新的缓冲区。
     *
     * @param bytes 源字节数组
     * @param offset 起始偏移量
     * @param length 复制长度
     * @return 包装复制内容的新缓冲区
     */
    static ByteBuffer wrapCopy(byte[] bytes, int offset, int length) {
        ByteBuffer buffer = ByteBuffer.allocate(length);
        buffer.put(bytes, offset, length);
        buffer.flip();
        return buffer;
    }
}
