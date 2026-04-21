package io.github.kongweiguang.socket.tcp.codec;

import io.github.kongweiguang.socket.tcp.exception.CodecException;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 基于分隔符的帧编解码器，解码后的消息不包含分隔符。
 *
 * @author kongweiguang
 */
public final class DelimiterCodec implements MessageCodec {

    private final byte[] delimiter;
    private final int maxFrameLength;

    /**
     * 创建分隔符编解码器。
     *
     * @param delimiter 分隔字节
     * @param maxFrameLength 最大帧长度
     */
    public DelimiterCodec(byte[] delimiter, int maxFrameLength) {
        if (delimiter == null || delimiter.length == 0) {
            throw new IllegalArgumentException("delimiter must not be empty");
        }
        if (maxFrameLength <= 0) {
            throw new IllegalArgumentException("maxFrameLength must be > 0");
        }
        this.delimiter = Arrays.copyOf(delimiter, delimiter.length);
        this.maxFrameLength = maxFrameLength;
    }

    /**
     * 创建分隔符编解码会话。
     *
     * @return 分隔符编解码会话
     */
    @Override
    public CodecSession newSession() {
        return new Session();
    }

    private final class Session implements CodecSession {

        private byte[] pending = new byte[0];

        @Override
        public ByteBuffer encode(ByteBuffer message) {
            ByteBuffer body = ByteBuffers.copy(message);
            ByteBuffer encoded = ByteBuffer.allocate(body.remaining() + delimiter.length);
            encoded.put(body);
            encoded.put(delimiter);
            encoded.flip();
            return encoded;
        }

        @Override
        public List<ByteBuffer> decode(ByteBuffer input) {
            byte[] incoming = ByteBuffers.readBytes(input);
            byte[] merged = new byte[pending.length + incoming.length];
            System.arraycopy(pending, 0, merged, 0, pending.length);
            System.arraycopy(incoming, 0, merged, pending.length, incoming.length);

            List<ByteBuffer> frames = new ArrayList<>();
            int start = 0;
            int index;
            while ((index = indexOf(merged, start)) >= 0) {
                int frameLength = index - start;
                if (frameLength > maxFrameLength) {
                    throw new CodecException("delimiter frame exceeds maxFrameLength: " + frameLength);
                }
                frames.add(ByteBuffers.wrapCopy(merged, start, frameLength));
                start = index + delimiter.length;
            }

            int remaining = merged.length - start;
            if (remaining > maxFrameLength) {
                throw new CodecException("delimiter frame exceeds maxFrameLength: " + remaining);
            }
            pending = Arrays.copyOfRange(merged, start, merged.length);
            return frames;
        }

        private int indexOf(byte[] bytes, int start) {
            outer:
            for (int i = start; i <= bytes.length - delimiter.length; i++) {
                for (int j = 0; j < delimiter.length; j++) {
                    if (bytes[i + j] != delimiter[j]) {
                        continue outer;
                    }
                }
                return i;
            }
            return -1;
        }
    }
}
