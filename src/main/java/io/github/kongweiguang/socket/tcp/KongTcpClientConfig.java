package io.github.kongweiguang.socket.tcp;

import io.github.kongweiguang.socket.tcp.codec.Codecs;
import io.github.kongweiguang.socket.tcp.codec.MessageCodec;

import java.net.InetSocketAddress;

/**
 * TCP 客户端不可变配置。
 *
 * @author kongweiguang
 */
public final class KongTcpClientConfig {

    private final InetSocketAddress remoteAddress;
    private final int bufferSize;
    private final int writeQueueSize;
    private final MessageCodec codec;
    private final TcpHandler handler;
    private final TcpClientListener listener;
    private final ReconnectConfig reconnectConfig;
    private final TcpDriver driver;

    KongTcpClientConfig(Builder builder) {
        this.remoteAddress = builder.remoteAddress;
        this.bufferSize = builder.bufferSize;
        this.writeQueueSize = builder.writeQueueSize;
        this.codec = builder.codec;
        this.handler = builder.handler;
        this.listener = builder.listener;
        this.reconnectConfig = builder.reconnectConfig;
        this.driver = builder.driver;
    }

    /**
     * 获取远端服务地址。
     *
     * @return 远端服务地址
     */
    public InetSocketAddress remoteAddress() {
        return remoteAddress;
    }

    /**
     * 获取单次读取使用的缓冲区大小。
     *
     * @return 缓冲区大小，单位字节
     */
    public int bufferSize() {
        return bufferSize;
    }

    /**
     * 获取单连接待写队列容量。
     *
     * @return 待写队列容量
     */
    public int writeQueueSize() {
        return writeQueueSize;
    }

    /**
     * 获取消息编解码器。
     *
     * @return 消息编解码器
     */
    public MessageCodec codec() {
        return codec;
    }

    /**
     * 获取 TCP 消息处理器。
     *
     * @return TCP 消息处理器
     */
    public TcpHandler handler() {
        return handler;
    }

    /**
     * 获取客户端连接状态监听器。
     *
     * @return 客户端连接状态监听器
     */
    public TcpClientListener listener() {
        return listener;
    }

    /**
     * 获取重连策略配置。
     *
     * @return 重连策略配置
     */
    public ReconnectConfig reconnectConfig() {
        return reconnectConfig;
    }

    /**
     * 获取底层 TCP IO 驱动。
     *
     * @return TCP IO 驱动
     */
    public TcpDriver driver() {
        return driver;
    }

    /**
     * TCP 客户端配置构建器。
     *
     * @author kongweiguang
     */
    public static final class Builder {

        private InetSocketAddress remoteAddress;
        private int bufferSize = 8 * 1024;
        private int writeQueueSize = 1024;
        private MessageCodec codec = Codecs.raw();
        private TcpHandler handler = (connection, message) -> {
        };
        private TcpClientListener listener = new TcpClientListener() {
        };
        private ReconnectConfig reconnectConfig = ReconnectConfig.disabled();
        private TcpDriver driver = TcpDriver.NIO;

        Builder() {
        }

        /**
         * 设置远端服务地址。
         *
         * @param host 远端主机名或 IP
         * @param port 远端端口
         * @return 当前构建器
         */
        public Builder remote(String host, int port) {
            if (host == null || host.isBlank()) {
                throw new IllegalArgumentException("host must not be blank");
            }
            KongTcpServerConfig.validatePort(port);
            this.remoteAddress = new InetSocketAddress(host, port);
            return this;
        }

        /**
         * 设置单次读取使用的缓冲区大小。
         *
         * @param bufferSize 缓冲区大小，单位字节
         * @return 当前构建器
         */
        public Builder bufferSize(int bufferSize) {
            if (bufferSize <= 0) {
                throw new IllegalArgumentException("bufferSize must be > 0");
            }
            this.bufferSize = bufferSize;
            return this;
        }

        /**
         * 设置单连接待写队列容量。
         *
         * @param writeQueueSize 待写队列容量
         * @return 当前构建器
         */
        public Builder writeQueueSize(int writeQueueSize) {
            if (writeQueueSize <= 0) {
                throw new IllegalArgumentException("writeQueueSize must be > 0");
            }
            this.writeQueueSize = writeQueueSize;
            return this;
        }

        /**
         * 设置消息编解码器。
         *
         * @param codec 消息编解码器
         * @return 当前构建器
         */
        public Builder codec(MessageCodec codec) {
            if (codec == null) {
                throw new IllegalArgumentException("codec must not be null");
            }
            this.codec = codec;
            return this;
        }

        /**
         * 设置 TCP 消息处理器。
         *
         * @param handler TCP 消息处理器
         * @return 当前构建器
         */
        public Builder handler(TcpHandler handler) {
            if (handler == null) {
                throw new IllegalArgumentException("handler must not be null");
            }
            this.handler = handler;
            return this;
        }

        /**
         * 设置客户端连接状态监听器。
         *
         * @param listener 客户端连接状态监听器
         * @return 当前构建器
         */
        public Builder listener(TcpClientListener listener) {
            if (listener == null) {
                throw new IllegalArgumentException("listener must not be null");
            }
            this.listener = listener;
            return this;
        }

        /**
         * 设置重连策略配置。
         *
         * @param reconnectConfig 重连策略配置
         * @return 当前构建器
         */
        public Builder reconnect(ReconnectConfig reconnectConfig) {
            if (reconnectConfig == null) {
                throw new IllegalArgumentException("reconnectConfig must not be null");
            }
            this.reconnectConfig = reconnectConfig;
            return this;
        }

        /**
         * 设置底层 TCP IO 驱动。
         *
         * @param driver TCP IO 驱动
         * @return 当前构建器
         */
        public Builder driver(TcpDriver driver) {
            if (driver == null) {
                throw new IllegalArgumentException("driver must not be null");
            }
            this.driver = driver;
            return this;
        }

        /**
         * 构建客户端不可变配置。
         *
         * @return 客户端不可变配置
         */
        public KongTcpClientConfig buildConfig() {
            if (remoteAddress == null) {
                throw new IllegalArgumentException("remote address is required");
            }
            return new KongTcpClientConfig(this);
        }

        /**
         * 构建 TCP 客户端实例。
         *
         * @return TCP 客户端实例
         */
        public KongTcpClient build() {
            return new KongTcpClient(buildConfig());
        }
    }
}
