package io.github.kongweiguang.socket.tcp.codec;

import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

/**
 * 内置消息编解码器工厂方法。
 *
 * @author kongweiguang
 */
public final class Codecs {

    private Codecs() {
    }

    /**
     * 原始字节编解码器，每次 socket 读取到的字节块都会作为一条消息。
     *
     * @return 原始字节编解码器
     */
    public static MessageCodec raw() {
        return RawCodec.INSTANCE;
    }

    /**
     * 换行符分隔编解码器。
     *
     * @return 换行符分隔编解码器
     */
    public static MessageCodec newline() {
        return delimiter("\n");
    }

    /**
     * 使用 UTF-8 分隔文本创建分隔符编解码器。
     *
     * @param delimiter 分隔文本
     * @return 分隔符编解码器
     */
    public static MessageCodec delimiter(String delimiter) {
        return delimiter(delimiter.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * 使用分隔字节创建分隔符编解码器。
     *
     * @param delimiter 分隔字节
     * @return 分隔符编解码器
     */
    public static MessageCodec delimiter(byte[] delimiter) {
        return new DelimiterCodec(delimiter, 1024 * 1024);
    }

    /**
     * 创建使用四字节大端长度字段作为帧头的编解码器。
     *
     * @return 长度字段编解码器
     */
    public static MessageCodec lengthField() {
        return lengthField(1024 * 1024);
    }

    /**
     * 创建使用四字节大端长度字段作为帧头的编解码器。
     *
     * @param maxFrameLength 最大帧长度
     * @return 长度字段编解码器
     */
    public static MessageCodec lengthField(int maxFrameLength) {
        return new LengthFieldCodec(maxFrameLength, ByteOrder.BIG_ENDIAN);
    }
}
