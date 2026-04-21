package io.github.kongweiguang.socket.tcp.codec;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * 单条连接独占的有状态编解码会话。
 *
 * @author kongweiguang
 */
public interface CodecSession {

    /**
     * 将一条出站消息编码为可写入 socket 的字节。
     *
     * @param message 出站消息
     * @return 编码后的字节
     */
    ByteBuffer encode(ByteBuffer message);

    /**
     * 将入站字节解码为零条或多条完整消息。
     *
     * @param input 入站字节
     * @return 已解码的完整消息列表
     */
    List<ByteBuffer> decode(ByteBuffer input);
}
