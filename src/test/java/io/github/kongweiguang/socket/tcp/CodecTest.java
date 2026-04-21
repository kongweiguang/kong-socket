package io.github.kongweiguang.socket.tcp;

import io.github.kongweiguang.socket.tcp.codec.CodecSession;
import io.github.kongweiguang.socket.tcp.codec.Codecs;
import io.github.kongweiguang.socket.tcp.codec.DelimiterCodec;
import io.github.kongweiguang.socket.tcp.codec.LengthFieldCodec;
import io.github.kongweiguang.socket.tcp.exception.CodecException;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 消息编解码器测试。
 *
 * @author kongweiguang
 */
class CodecTest {

    @Test
    void rawCodecReturnsReadChunk() {
        CodecSession session = Codecs.raw().newSession();

        List<ByteBuffer> messages = session.decode(bytes("hello"));

        assertEquals(List.of("hello"), strings(messages));
    }

    @Test
    void delimiterCodecHandlesCompleteHalfAndStickyFrames() {
        CodecSession session = Codecs.newline().newSession();

        assertTrue(session.decode(bytes("he")).isEmpty());
        List<ByteBuffer> messages = session.decode(bytes("llo\none\ntwo"));
        assertEquals(List.of("hello", "one"), strings(messages));

        messages = session.decode(bytes("\n"));
        assertEquals(List.of("two"), strings(messages));
    }

    @Test
    void delimiterCodecRejectsOversizedFrame() {
        CodecSession session = new DelimiterCodec(new byte[]{'\n'}, 3).newSession();

        assertThrows(CodecException.class, () -> session.decode(bytes("abcd")));
    }

    @Test
    void delimiterCodecEncodesDelimiter() {
        CodecSession session = Codecs.newline().newSession();

        assertEquals("hello\n", string(session.encode(bytes("hello"))));
    }

    @Test
    void lengthFieldCodecHandlesCompleteHalfAndStickyFrames() {
        CodecSession session = Codecs.lengthField().newSession();
        ByteBuffer one = session.encode(bytes("one"));
        ByteBuffer two = session.encode(bytes("two"));
        ByteBuffer merged = ByteBuffer.allocate(one.remaining() + two.remaining());
        merged.put(one).put(two).flip();

        ByteBuffer firstHalf = ByteBuffer.allocate(5);
        for (int i = 0; i < 5; i++) {
            firstHalf.put(merged.get());
        }
        firstHalf.flip();

        assertTrue(session.decode(firstHalf).isEmpty());
        assertEquals(List.of("one", "two"), strings(session.decode(merged)));
    }

    @Test
    void lengthFieldCodecRejectsInvalidLength() {
        CodecSession session = new LengthFieldCodec(8, ByteOrder.BIG_ENDIAN).newSession();
        ByteBuffer invalid = ByteBuffer.allocate(4).putInt(9).flip();

        assertThrows(CodecException.class, () -> session.decode(invalid));
    }

    private ByteBuffer bytes(String value) {
        return ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8));
    }

    private String string(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private List<String> strings(List<ByteBuffer> buffers) {
        return buffers.stream().map(this::string).toList();
    }
}
