package io.github.kongweiguang.socket.tcp.codec;

/**
 * 为每条连接创建相互独立的编解码会话。
 *
 * @author kongweiguang
 */
public interface MessageCodec {

    /**
     * 创建编解码会话。
     *
     * @return 编解码会话
     */
    CodecSession newSession();
}
