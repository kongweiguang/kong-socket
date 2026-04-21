package io.github.kongweiguang.socket.tcp;

import io.github.kongweiguang.socket.tcp.*;
import io.github.kongweiguang.socket.tcp.codec.Codecs;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * TCP 服务端和客户端集成行为测试。
 *
 * @author kongweiguang
 */
class TcpIntegrationTest {

    @ParameterizedTest
    @EnumSource(TcpDriver.class)
    void serverClientEchoRoundTripWithDelimiterCodec(TcpDriver driver) throws Exception {
        try (KongTcpServer server = echoServer(driver, Codecs.newline(), 3, 1024)) {
            server.start();
            InetSocketAddress address = server.boundAddresses().getFirst();
            CountDownLatch latch = new CountDownLatch(1);
            AtomicReference<String> received = new AtomicReference<>();

            try (KongTcpClient client = KongTcpClient.builder()
                    .remote("127.0.0.1", address.getPort())
                    .driver(driver)
                    .codec(Codecs.newline())
                    .handler(messageHandler((connection, message) -> {
                        received.set(text(message));
                        latch.countDown();
                    }))
                    .build()) {
                Connection connection = client.connect().get(3, TimeUnit.SECONDS);
                connection.send("hello").get(3, TimeUnit.SECONDS);

                assertTrue(latch.await(3, TimeUnit.SECONDS));
                assertEquals("hello", received.get());
            }
        }
    }

    @ParameterizedTest
    @EnumSource(TcpDriver.class)
    void supportsMultipleClientsAndNonPowerOfTwoWorkers(TcpDriver driver) throws Exception {
        try (KongTcpServer server = echoServer(driver, Codecs.lengthField(), 3, 2048)) {
            server.start();
            InetSocketAddress address = server.boundAddresses().getFirst();
            int clients = 8;
            CountDownLatch latch = new CountDownLatch(clients);
            List<KongTcpClient> openClients = new ArrayList<>();
            List<CompletableFuture<Void>> sends = new ArrayList<>();

            try {
                for (int i = 0; i < clients; i++) {
                    int index = i;
                    KongTcpClient client = KongTcpClient.builder()
                            .remote("127.0.0.1", address.getPort())
                            .driver(driver)
                            .codec(Codecs.lengthField())
                            .handler(messageHandler((connection, message) -> {
                                if (("msg-" + index).equals(text(message))) {
                                    latch.countDown();
                                }
                            }))
                            .build();
                    openClients.add(client);
                    Connection connection = client.connect().get(3, TimeUnit.SECONDS);
                    sends.add(connection.send("msg-" + index));
                }

                CompletableFuture.allOf(sends.toArray(CompletableFuture[]::new)).get(3, TimeUnit.SECONDS);
                assertTrue(latch.await(3, TimeUnit.SECONDS));
            } finally {
                openClients.forEach(KongTcpClient::close);
            }
        }
    }

    @ParameterizedTest
    @EnumSource(TcpDriver.class)
    void largeMessageRoundTripUsesLengthFieldCodec(TcpDriver driver) throws Exception {
        String payload = "x".repeat(128 * 1024);
        try (KongTcpServer server = echoServer(driver, Codecs.lengthField(256 * 1024), 2, 512)) {
            server.start();
            InetSocketAddress address = server.boundAddresses().getFirst();
            CountDownLatch latch = new CountDownLatch(1);
            AtomicReference<Integer> size = new AtomicReference<>();

            try (KongTcpClient client = KongTcpClient.builder()
                    .remote("127.0.0.1", address.getPort())
                    .driver(driver)
                    .codec(Codecs.lengthField(256 * 1024))
                    .bufferSize(512)
                    .handler(messageHandler((connection, message) -> {
                        size.set(message.remaining());
                        latch.countDown();
                    }))
                    .build()) {
                Connection connection = client.connect().get(3, TimeUnit.SECONDS);
                connection.send(payload).get(3, TimeUnit.SECONDS);

                assertTrue(latch.await(5, TimeUnit.SECONDS));
                assertEquals(payload.length(), size.get());
            }
        }
    }

    @ParameterizedTest
    @EnumSource(TcpDriver.class)
    void closeReleasesPortAndClosedConnectionRejectsSend(TcpDriver driver) throws Exception {
        KongTcpServer server = echoServer(driver, Codecs.raw(), 1, 1024);
        server.start();
        int port = server.boundAddresses().getFirst().getPort();

        KongTcpClient client = KongTcpClient.builder()
                .remote("127.0.0.1", port)
                .driver(driver)
                .codec(Codecs.raw())
                .build();
        Connection connection = client.connect().get(3, TimeUnit.SECONDS);
        connection.close();
        assertThrows(Exception.class, () -> connection.send("after-close").get(1, TimeUnit.SECONDS));
        client.close();
        server.close();

        assertCanBind(port);
    }

    @ParameterizedTest
    @EnumSource(TcpDriver.class)
    void clientReportsConnectFailureAndReconnectExhaustion(TcpDriver driver) throws Exception {
        int port = freePort();
        CountDownLatch reconnectFailed = new CountDownLatch(1);
        AtomicInteger failures = new AtomicInteger();

        try (KongTcpClient client = KongTcpClient.builder()
                .remote("127.0.0.1", port)
                .driver(driver)
                .reconnect(ReconnectConfig.of(2, Duration.ofMillis(10), Duration.ofMillis(20)))
                .listener(new TcpClientListener() {
                    @Override
                    public void onConnectFailed(InetSocketAddress remote, int attempt, Throwable error) {
                        failures.incrementAndGet();
                    }

                    @Override
                    public void onReconnectFailed(InetSocketAddress remote, int attempts, Throwable lastError) {
                        reconnectFailed.countDown();
                    }
                })
                .build()) {
            assertThrows(Exception.class, () -> client.connect().get(2, TimeUnit.SECONDS));
            assertTrue(reconnectFailed.await(3, TimeUnit.SECONDS));
            assertTrue(failures.get() >= 3);
        }
    }

    @ParameterizedTest
    @EnumSource(value = TcpDriver.class, names = "AIO")
    void aioHandlerExceptionTriggersErrorAndClose(TcpDriver driver) throws Exception {
        CountDownLatch error = new CountDownLatch(1);
        CountDownLatch close = new CountDownLatch(1);
        try (KongTcpServer server = KongTcpServer.builder()
                .bind("127.0.0.1", 0)
                .driver(driver)
                .codec(Codecs.newline())
                .handler(new TcpHandler() {
                    @Override
                    public void onMessage(Connection connection, ByteBuffer message) {
                        throw new IllegalStateException("boom");
                    }

                    @Override
                    public void onError(Connection connection, Throwable cause) {
                        error.countDown();
                    }

                    @Override
                    public void onClose(Connection connection) {
                        close.countDown();
                    }
                })
                .build()) {
            server.start();
            InetSocketAddress address = server.boundAddresses().getFirst();
            try (KongTcpClient client = KongTcpClient.builder()
                    .remote("127.0.0.1", address.getPort())
                    .driver(driver)
                    .codec(Codecs.newline())
                    .build()) {
                Connection connection = client.connect().get(3, TimeUnit.SECONDS);
                connection.send("boom").get(3, TimeUnit.SECONDS);

                assertTrue(error.await(3, TimeUnit.SECONDS));
                assertTrue(close.await(3, TimeUnit.SECONDS));
            }
        }
    }

    private KongTcpServer echoServer(TcpDriver driver,
                                     io.github.kongweiguang.socket.tcp.codec.MessageCodec codec,
                                     int workers,
                                     int bufferSize) {
        return KongTcpServer.builder()
                .bind("127.0.0.1", 0)
                .driver(driver)
                .workerThreads(workers)
                .bufferSize(bufferSize)
                .codec(codec)
                .handler(messageHandler(Connection::send))
                .build();
    }

    private TcpHandler messageHandler(MessageCallback callback) {
        return new TcpHandler() {
            @Override
            public void onMessage(Connection connection, ByteBuffer message) {
                callback.onMessage(connection, message);
            }
        };
    }

    private String text(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private void assertCanBind(int port) throws IOException {
        try (ServerSocket ignored = new ServerSocket(port)) {
            assertEquals(port, ignored.getLocalPort());
        }
    }

    /**
     * 测试用消息回调。
     */
    private interface MessageCallback {
        void onMessage(Connection connection, ByteBuffer message);
    }
}
