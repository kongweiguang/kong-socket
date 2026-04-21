# kong-socket

`kong-socket` 是一个轻量级 TCP 客户端/服务端库，提供 NIO 和 AIO 两种底层驱动，并内置常见的消息拆包编解码器。你可以用它快速搭建 TCP 服务、连接远端服务、发送文本或二进制消息。

## 环境要求

- JDK 21+
- Maven 3.8+

## 安装

当前模块的 Maven 坐标来自 `pom.xml`：

```xml
<dependency>
    <groupId>io.github.kongweiguang</groupId>
    <artifactId>kong-socket</artifactId>
    <version>0.6</version>
</dependency>
```

如果是在源码仓库中使用，可以先运行测试确认环境可用：

```bash
mvn test
```

## 5 分钟快速入门

下面示例启动一个 TCP echo 服务端，然后创建客户端连接它。服务端收到一条完整消息后原样写回，客户端收到回包后打印内容。

```java
import io.github.kongweiguang.socket.tcp.Connection;
import io.github.kongweiguang.socket.tcp.KongTcpClient;
import io.github.kongweiguang.socket.tcp.KongTcpServer;
import io.github.kongweiguang.socket.tcp.TcpDriver;
import io.github.kongweiguang.socket.tcp.codec.Codecs;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class QuickStart {

    public static void main(String[] args) throws Exception {
        CountDownLatch received = new CountDownLatch(1);

        try (KongTcpServer server = KongTcpServer.builder()
                .bind("127.0.0.1", 0)
                .driver(TcpDriver.NIO)
                .codec(Codecs.newline())
                .handler((connection, message) -> connection.send(message))
                .build()) {

            server.start();
            InetSocketAddress address = server.boundAddresses().getFirst();

            try (KongTcpClient client = KongTcpClient.builder()
                    .remote("127.0.0.1", address.getPort())
                    .driver(TcpDriver.NIO)
                    .codec(Codecs.newline())
                    .handler((connection, message) -> {
                        System.out.println("server reply: " + toText(message));
                        received.countDown();
                    })
                    .build()) {

                Connection connection = client.connect().get(3, TimeUnit.SECONDS);
                connection.send("hello").get(3, TimeUnit.SECONDS);

                if (!received.await(3, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("receive timeout");
                }
            }
        }
    }

    private static String toText(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
```

运行后会看到：

```text
server reply: hello
```

## 服务端用法

服务端至少需要配置一个监听地址。`bind("127.0.0.1", 0)` 会让系统自动分配端口，启动后可通过 `boundAddresses()` 获取真实端口。

```java
KongTcpServer server = KongTcpServer.builder()
        .bind("0.0.0.0", 9000)
        .workerThreads(3)
        .bufferSize(8 * 1024)
        .codec(Codecs.lengthField())
        .handler((connection, message) -> {
            // message 是已经完成拆包后的完整消息。
            connection.send(message);
        })
        .build();

server.start();
```

常用配置：

- `bind(host, port)`：监听指定地址和端口，端口 `0` 表示自动分配。
- `workerThreads(n)`：设置 worker 线程数，必须大于 `0`。
- `bufferSize(bytes)`：设置单次读取缓冲区大小，必须大于 `0`。
- `writeQueueSize(n)`：设置单连接待写队列容量。
- `driver(TcpDriver.NIO)` / `driver(TcpDriver.AIO)`：选择底层 IO 驱动，默认是 `NIO`。
- `codec(...)`：设置消息编解码器，默认是 `Codecs.raw()`。

## 客户端用法

客户端至少需要配置远端地址。`connect()` 返回 `CompletableFuture<Connection>`，连接成功后可以通过 `Connection` 异步发送消息。

```java
try (KongTcpClient client = KongTcpClient.builder()
        .remote("127.0.0.1", 9000)
        .codec(Codecs.lengthField())
        .handler((connection, message) -> {
            System.out.println("receive " + message.remaining() + " bytes");
        })
        .build()) {

    Connection connection = client.connect().get(3, TimeUnit.SECONDS);
    connection.send("hello").get(3, TimeUnit.SECONDS);
}
```

发送方法：

- `connection.send(String message)`：按 UTF-8 发送文本。
- `connection.send(String message, Charset charset)`：按指定字符集发送文本。
- `connection.send(byte[] message)`：发送字节数组。
- `connection.send(ByteBuffer message)`：发送二进制缓冲区。

## 消息编解码器

TCP 是字节流协议，业务通常需要处理半包和粘包。本库通过 `MessageCodec` 把字节流拆成完整消息，服务端和客户端必须使用一致的编解码协议。

内置编解码器：

- `Codecs.raw()`：每次 socket 读取到的字节块作为一条消息。适合简单测试，不适合有明确消息边界的业务协议。
- `Codecs.newline()`：使用换行符 `\n` 分隔消息。发送 `"hello"` 时会自动编码为 `"hello\n"`。
- `Codecs.delimiter(";")`：使用自定义 UTF-8 文本分隔符。
- `Codecs.lengthField()`：使用 4 字节大端长度字段作为消息头，默认最大帧长度为 `1 MB`。
- `Codecs.lengthField(maxFrameLength)`：自定义最大帧长度，适合大消息传输。

大消息示例：

```java
KongTcpServer server = KongTcpServer.builder()
        .bind("127.0.0.1", 9000)
        .codec(Codecs.lengthField(256 * 1024))
        .bufferSize(512)
        .handler((connection, message) -> connection.send(message))
        .build();
```

## 连接事件和异常处理

`TcpHandler` 可以处理连接生命周期和业务异常：

```java
TcpHandler handler = new TcpHandler() {
    @Override
    public void onConnect(Connection connection) {
        System.out.println("connected: " + connection.remoteAddress());
    }

    @Override
    public void onMessage(Connection connection, ByteBuffer message) {
        connection.send(message);
    }

    @Override
    public void onError(Connection connection, Throwable error) {
        error.printStackTrace();
    }

    @Override
    public void onClose(Connection connection) {
        System.out.println("closed");
    }
};
```

客户端还可以通过 `TcpClientListener` 观察连接、重连和断开事件：

```java
KongTcpClient client = KongTcpClient.builder()
        .remote("127.0.0.1", 9000)
        .reconnect(ReconnectConfig.of(
                2,
                Duration.ofMillis(100),
                Duration.ofSeconds(1)))
        .listener(new TcpClientListener() {
            @Override
            public void onConnectFailed(InetSocketAddress remote, int attempt, Throwable error) {
                System.out.println("connect failed, attempt=" + attempt);
            }

            @Override
            public void onReconnectFailed(InetSocketAddress remote, int attempts, Throwable lastError) {
                System.out.println("reconnect exhausted, attempts=" + attempts);
            }
        })
        .build();
```

`ReconnectConfig.of(maxAttempts, initialDelay, maxDelay)` 表示首次连接失败后最多重试 `maxAttempts` 次，重试延迟按指数增长并被 `maxDelay` 限制。

## 资源释放

`KongTcpServer`、`KongTcpClient` 和 `Connection` 都实现了 `AutoCloseable`。推荐使用 `try-with-resources` 自动释放资源：

```java
try (KongTcpServer server = KongTcpServer.builder()
        .bind("127.0.0.1", 9000)
        .build()) {
    server.start();
}
```

连接关闭后继续 `send(...)` 会失败，调用方应在发送前检查 `connection.isOpen()`，或处理 `CompletableFuture` 中的异常。

## 运行测试

```bash
mvn test
```

测试覆盖了以下行为：

- NIO/AIO 两种驱动下的服务端和客户端 echo 通信。
- 多客户端并发发送。
- 换行符和长度字段编解码器的半包、粘包处理。
- 大消息传输。
- 配置校验、连接失败、重连耗尽和关闭资源。
