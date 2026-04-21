package io.github.kongweiguang.socket.tcp;

import io.github.kongweiguang.socket.tcp.codec.Codecs;
import io.github.kongweiguang.socket.tcp.codec.MessageCodec;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * TCP 服务端不可变配置。
 *
 * @author kongweiguang
 */
public final class KongTcpServerConfig {

    private final List<InetSocketAddress> bindAddresses;
    private final int bossThreads;
    private final int workerThreads;
    private final int bufferSize;
    private final int writeQueueSize;
    private final MessageCodec codec;
    private final TcpHandler handler;
    private final TcpDriver driver;

    KongTcpServerConfig(Builder builder) {
        this.bindAddresses = List.copyOf(builder.bindAddresses);
        this.bossThreads = builder.bossThreads;
        this.workerThreads = builder.workerThreads;
        this.bufferSize = builder.bufferSize;
        this.writeQueueSize = builder.writeQueueSize;
        this.codec = builder.codec;
        this.handler = builder.handler;
        this.driver = builder.driver;
    }

    /**
     * 获取服务端监听地址列表。
     *
     * @return 服务端监听地址列表
     */
    public List<InetSocketAddress> bindAddresses() {
        return Collections.unmodifiableList(bindAddresses);
    }

    /**
     * 获取 boss 线程数量。
     *
     * @return boss 线程数量
     */
    public int bossThreads() {
        return bossThreads;
    }

    /**
     * 获取 worker 线程数量。
     *
     * @return worker 线程数量
     */
    public int workerThreads() {
        return workerThreads;
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
     * 获取底层 TCP IO 驱动。
     *
     * @return TCP IO 驱动
     */
    public TcpDriver driver() {
        return driver;
    }

    /**
     * TCP 服务端配置构建器。
     *
     * @author kongweiguang
     */
    public static final class Builder {

        private final List<InetSocketAddress> bindAddresses = new ArrayList<>();
        private int bossThreads = 1;
        private int workerThreads = Math.max(1, Runtime.getRuntime().availableProcessors() * 2);
        private int bufferSize = 8 * 1024;
        private int writeQueueSize = 1024;
        private MessageCodec codec = Codecs.raw();
        private TcpHandler handler = (connection, message) -> {
        };
        private TcpDriver driver = TcpDriver.NIO;

        Builder() {
        }

        /**
         * 监听指定端口，默认绑定到全部网卡。
         *
         * @param port 监听端口
         * @return 当前构建器
         */
        public Builder bind(int port) {
            return bind("0.0.0.0", port);
        }

        /**
         * 监听指定主机和端口。
         *
         * @param host 绑定主机名或 IP
         * @param port 监听端口
         * @return 当前构建器
         */
        public Builder bind(String host, int port) {
            if (host == null || host.isBlank()) {
                throw new IllegalArgumentException("host must not be blank");
            }
            validatePort(port);
            bindAddresses.add(new InetSocketAddress(host, port));
            return this;
        }

        /**
         * 设置 boss 线程数量。
         *
         * @param bossThreads boss 线程数量
         * @return 当前构建器
         */
        public Builder bossThreads(int bossThreads) {
            if (bossThreads <= 0) {
                throw new IllegalArgumentException("bossThreads must be > 0");
            }
            this.bossThreads = bossThreads;
            return this;
        }

        /**
         * 设置 worker 线程数量。
         *
         * @param workerThreads worker 线程数量
         * @return 当前构建器
         */
        public Builder workerThreads(int workerThreads) {
            if (workerThreads <= 0) {
                throw new IllegalArgumentException("workerThreads must be > 0");
            }
            this.workerThreads = workerThreads;
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
         * 构建服务端不可变配置。
         *
         * @return 服务端不可变配置
         */
        public KongTcpServerConfig buildConfig() {
            if (bindAddresses.isEmpty()) {
                throw new IllegalArgumentException("at least one bind address is required");
            }
            return new KongTcpServerConfig(this);
        }

        /**
         * 构建 TCP 服务端实例。
         *
         * @return TCP 服务端实例
         */
        public KongTcpServer build() {
            return new KongTcpServer(buildConfig());
        }
    }

    static void validatePort(int port) {
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("port must be between 0 and 65535");
        }
    }
}
