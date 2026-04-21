package io.github.kongweiguang.socket.tcp.codec;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

/**
 * 原始字节编解码器。
 *
 * @author kongweiguang
 */
public final class RawCodec implements MessageCodec {

    static final RawCodec INSTANCE = new RawCodec();

    private RawCodec() {
    }

    /**
     * 创建原始字节编解码会话。
     *
     * @return 原始字节编解码会话
     */
    @Override
    public CodecSession newSession() {
        return new Session();
    }

    private static final class Session implements CodecSession {

        @Override
        public ByteBuffer encode(ByteBuffer message) {
            return ByteBuffers.copy(message);
        }

        @Override
        public List<ByteBuffer> decode(ByteBuffer input) {
            if (!input.hasRemaining()) {
                return Collections.emptyList();
            }
            return List.of(ByteBuffers.copy(input));
        }
    }
}
