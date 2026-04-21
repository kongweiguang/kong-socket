package io.github.kongweiguang.socket.tcp.exception;

/**
 * kong-socket 模块的基础运行时异常。
 *
 * @author kongweiguang
 */
public class KongSocketException extends RuntimeException {

    /**
     * 创建带错误消息的 socket 异常。
     *
     * @param message 错误消息
     */
    public KongSocketException(String message) {
        super(message);
    }

    /**
     * 创建带错误消息和原因异常的 socket 异常。
     *
     * @param message 错误消息
     * @param cause 原因异常
     */
    public KongSocketException(String message, Throwable cause) {
        super(message, cause);
    }
}
