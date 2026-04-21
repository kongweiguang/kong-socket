package io.github.kongweiguang.socket.tcp.exception;

/**
 * 在已关闭连接上发送消息或访问连接资源时抛出的异常。
 *
 * @author kongweiguang
 */
public class ConnectionClosedException extends KongSocketException {

    /**
     * 创建连接已关闭异常。
     *
     * @param message 错误消息
     */
    public ConnectionClosedException(String message) {
        super(message);
    }
}
