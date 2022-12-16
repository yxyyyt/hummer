package com.sciatta.hummer.core.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by Rain on 2022/12/16<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 服务抽象实现
 */
public abstract class AbstractServer implements Server {

    private static final Logger logger = LoggerFactory.getLogger(AbstractServer.class);

    /**
     * 阻塞器
     */
    private final Lock LOCK = new ReentrantLock();
    private final Condition KEEPER = LOCK.newCondition();

    /**
     * 正在启动
     */
    private static final AtomicBoolean STARTING = new AtomicBoolean(false);

    /**
     * 启动成功
     */
    private static final AtomicBoolean STARTED = new AtomicBoolean(false);

    /**
     * 正在关闭
     */
    private static final AtomicBoolean CLOSING = new AtomicBoolean(false);

    /**
     * 关闭成功
     */
    private static final AtomicBoolean CLOSED = new AtomicBoolean(false);

    @Override
    public final Server start() throws IOException {
        // 只允许启动一次
        if (!STARTING.compareAndSet(false, true)) {
            return this;
        }

        doStart();

        registerShutdownHook();

        STARTED.set(true);

        logger.info("{} started", AbstractServer.this.getClass().getSimpleName());

        return this;
    }

    @Override
    public final void close() {
        // 只允许关闭一次
        if (!CLOSING.compareAndSet(false, true)) {
            return;
        }

        doClose();

        // 唤醒等待服务
        leave();

        CLOSED.set(true);

        logger.info("{} closed", AbstractServer.this.getClass().getSimpleName());
    }

    @Override
    public final void keep() {
        LOCK.lock();
        try {
            KEEPER.await();
        } catch (InterruptedException ignore) {
        } finally {
            LOCK.unlock();
        }
    }

    @Override
    public final boolean isStarted() {
        return STARTED.get();
    }

    @Override
    public final boolean isClosing() {
        return CLOSING.get();
    }

    /**
     * JVM进程结束时运行钩子回调
     */
    private void registerShutdownHook() {
        // 注册回调钩子
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.warn("*** shutting down {} since JVM is shutting down", AbstractServer.this.getClass().getSimpleName());
            AbstractServer.this.close();
            logger.warn("*** {} shut down", AbstractServer.this.getClass().getSimpleName());
        }));
    }

    /**
     * 唤醒等待服务
     */
    private void leave() {
        LOCK.lock();
        try {
            KEEPER.signal();
        } finally {
            LOCK.unlock();
        }
    }

    /**
     * 启动过程由子类实现
     */
    protected abstract void doStart() throws IOException;

    /**
     * 关闭过程由子类实现
     */
    protected abstract void doClose();
}
