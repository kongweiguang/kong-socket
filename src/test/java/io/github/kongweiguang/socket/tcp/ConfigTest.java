package io.github.kongweiguang.socket.tcp;

import io.github.kongweiguang.socket.tcp.*;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

/**
 * TCP 客户端和服务端配置校验测试。
 *
 * @author kongweiguang
 */
class ConfigTest {

    @Test
    void serverConfigValidatesRequiredAddressAndPositiveValues() {
        assertThrows(IllegalArgumentException.class, () -> KongTcpServer.builder().build());
        assertThrows(IllegalArgumentException.class, () -> KongTcpServer.builder().bind("127.0.0.1", -1));
        assertThrows(IllegalArgumentException.class, () -> KongTcpServer.builder().bind("127.0.0.1", 0).workerThreads(0));
        assertThrows(IllegalArgumentException.class, () -> KongTcpServer.builder().bind("127.0.0.1", 0).bufferSize(0));

        KongTcpServerConfig config = KongTcpServer.builder()
                .bind("127.0.0.1", 0)
                .workerThreads(3)
                .build()
                .config();

        assertEquals(3, config.workerThreads());
    }

    @Test
    void clientConfigValidatesRequiredRemoteAndReconnect() {
        assertThrows(IllegalArgumentException.class, () -> KongTcpClient.builder().build());
        assertThrows(IllegalArgumentException.class, () -> KongTcpClient.builder().remote("", 80));
        assertThrows(IllegalArgumentException.class, () -> ReconnectConfig.of(-1, Duration.ZERO, Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> ReconnectConfig.of(1, Duration.ofSeconds(2), Duration.ofSeconds(1)));

        KongTcpClientConfig config = KongTcpClient.builder()
                .remote("127.0.0.1", 80)
                .reconnect(ReconnectConfig.of(2, Duration.ofMillis(10), Duration.ofMillis(20)))
                .build()
                .config();

        assertTrue(config.reconnectConfig().enabled());
        assertEquals(Duration.ofMillis(20), config.reconnectConfig().delayFor(2));
    }

    @Test
    void serverAndClientConfigSupportDriverSelection() {
        KongTcpServerConfig serverDefault = KongTcpServer.builder()
                .bind("127.0.0.1", 0)
                .build()
                .config();
        KongTcpClientConfig clientDefault = KongTcpClient.builder()
                .remote("127.0.0.1", 80)
                .build()
                .config();

        assertEquals(TcpDriver.NIO, serverDefault.driver());
        assertEquals(TcpDriver.NIO, clientDefault.driver());

        KongTcpServerConfig aioServer = KongTcpServer.builder()
                .bind("127.0.0.1", 0)
                .driver(TcpDriver.AIO)
                .build()
                .config();
        KongTcpClientConfig aioClient = KongTcpClient.builder()
                .remote("127.0.0.1", 80)
                .driver(TcpDriver.AIO)
                .build()
                .config();

        assertEquals(TcpDriver.AIO, aioServer.driver());
        assertEquals(TcpDriver.AIO, aioClient.driver());
        assertThrows(IllegalArgumentException.class, () -> KongTcpServer.builder().driver(null));
        assertThrows(IllegalArgumentException.class, () -> KongTcpClient.builder().driver(null));
    }
}
