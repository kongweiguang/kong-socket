package io.github.kongweiguang.socket.tcp.codec;

import io.github.kongweiguang.socket.tcp.exception.CodecException;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 使用四字节长度字段的帧编解码器。
 *
 * @author kongweiguang
 */
public final class LengthFieldCodec implements MessageCodec {

    private static final int LENGTH_FIELD_SIZE = Integer.BYTES;

    private final int maxFrameLength;
    private final ByteOrder byteOrder;

    /**
     * 创建长度字段编解码器。
     *
     * @param maxFrameLength 最大帧长度
     * @param byteOrder 长度字段字节序
     */
    public LengthFieldCodec(int maxFrameLength, ByteOrder byteOrder) {
        if (maxFrameLength <= 0) {
            throw new IllegalArgumentException("maxFrameLength must be > 0");
        }
        if (byteOrder == null) {
            throw new IllegalArgumentException("byteOrder must not be null");
        }
        this.maxFrameLength = maxFrameLength;
        this.byteOrder = byteOrder;
    }

    /**
     * 创建长度字段编解码会话。
     *
     * @return 长度字段编解码会话
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
            int length = body.remaining();
            if (length > maxFrameLength) {
                throw new CodecException("length-field frame exceeds maxFrameLength: " + length);
            }
            ByteBuffer encoded = ByteBuffer.allocate(LENGTH_FIELD_SIZE + length).order(byteOrder);
            encoded.putInt(length);
            encoded.put(body);
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
            int offset = 0;
            while (merged.length - offset >= LENGTH_FIELD_SIZE) {
                ByteBuffer header = ByteBuffer.wrap(merged, offset, LENGTH_FIELD_SIZE).order(byteOrder);
                int length = header.getInt();
                if (length < 0 || length > maxFrameLength) {
                    throw new CodecException("invalid length-field frame length: " + length);
                }
                if (merged.length - offset - LENGTH_FIELD_SIZE < length) {
                    break;
                }
                frames.add(ByteBuffers.wrapCopy(merged, offset + LENGTH_FIELD_SIZE, length));
                offset += LENGTH_FIELD_SIZE + length;
            }

            pending = Arrays.copyOfRange(merged, offset, merged.length);
            return frames;
        }
    }
}
