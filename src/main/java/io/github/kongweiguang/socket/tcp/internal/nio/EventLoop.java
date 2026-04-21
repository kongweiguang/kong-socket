package io.github.kongweiguang.socket.tcp.internal.nio;

import io.github.kongweiguang.socket.tcp.exception.KongSocketException;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 单线程 NIO 事件循环，负责执行跨线程投递任务和处理 selector 就绪事件。
 *
 * @author kongweiguang
 */
abstract class EventLoop implements AutoCloseable {

    private final Selector selector;
    private final Queue<Runnable> tasks = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final Thread thread;

    EventLoop(String name) {
        try {
            this.selector = Selector.open();
        } catch (IOException e) {
            throw new KongSocketException("open selector failed", e);
        }
        this.thread = Thread.ofPlatform().name(name).daemon(true).unstarted(this::run);
    }

    /**
     * 获取当前事件循环持有的 selector。
     *
     * @return selector 实例
     */
    final Selector selector() {
        return selector;
    }

    /**
     * 判断当前线程是否为事件循环线程。
     *
     * @return 在事件循环线程内返回 true
     */
    final boolean inEventLoop() {
        return Thread.currentThread() == thread;
    }

    /**
     * 判断事件循环是否正在运行。
     *
     * @return 运行中返回 true
     */
    final boolean isRunning() {
        return running.get();
    }

    /**
     * 启动事件循环线程。
     */
    final void start() {
        if (running.compareAndSet(false, true)) {
            thread.start();
        }
    }

    /**
     * 投递任务到事件循环线程执行。
     *
     * @param task 待执行任务
     */
    final void execute(Runnable task) {
        tasks.offer(task);
        selector.wakeup();
    }

    private void run() {
        while (running.get()) {
            try {
                runTasks();
                selector.select(1000);
                Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();
                while (iterator.hasNext()) {
                    SelectionKey key = iterator.next();
                    iterator.remove();
                    if (key.isValid()) {
                        handle(key);
                    }
                }
                runTasks();
            } catch (IOException | RuntimeException e) {
                onLoopError(e);
            }
        }
        closeSelectorKeys();
    }

    private void runTasks() {
        Runnable task;
        while ((task = tasks.poll()) != null) {
            try {
                task.run();
            } catch (RuntimeException e) {
                onLoopError(e);
            }
        }
    }

    private void closeSelectorKeys() {
        for (SelectionKey key : selector.keys()) {
            try {
                key.channel().close();
            } catch (IOException ignored) {
            }
            key.cancel();
        }
        try {
            selector.close();
        } catch (IOException ignored) {
        }
    }

    /**
     * 处理单个 selector 就绪事件。
     *
     * @param key 就绪 selection key
     * @throws IOException IO 异常
     */
    protected abstract void handle(SelectionKey key) throws IOException;

    /**
     * 处理事件循环中未被业务逻辑消费的异常。
     *
     * @param error 异常信息
     */
    protected abstract void onLoopError(Throwable error);

    /**
     * 请求事件循环停止并唤醒 selector。
     */
    @Override
    public void close() {
        running.set(false);
        selector.wakeup();
    }
}
