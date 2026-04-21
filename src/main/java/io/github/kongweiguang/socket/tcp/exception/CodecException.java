package io.github.kongweiguang.socket.tcp.exception;

/**
 * 编解码器无法编码或解码数据帧时抛出的异常。
 *
 * @author kongweiguang
 */
public class CodecException extends KongSocketException {

    /**
     * 创建编解码异常。
     *
     * @param message 错误消息
     */
    public CodecException(String message) {
        super(message);
    }

    /**
     * 创建带原因异常的编解码异常。
     *
     * @param message 错误消息
     * @param cause 原因异常
     */
    public CodecException(String message, Throwable cause) {
        super(message, cause);
    }
}
