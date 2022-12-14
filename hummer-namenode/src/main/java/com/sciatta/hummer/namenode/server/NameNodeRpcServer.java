package com.sciatta.hummer.namenode.server;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.sciatta.hummer.namenode.server.NameNodeConfig.NAME_NODE_RPC_SERVER_PORT;

/**
 * Created by Rain on 2022/12/13<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 元数据节点RPC服务端，对外提供RPC服务，使用Grpc实现
 */
public class NameNodeRpcServer {

    private static final Logger logger = LoggerFactory.getLogger(NameNodeRpcServer.class);

    /**
     * grpc internal server
     */
    private Server server = null;

    /**
     * 管理元数据
     */
    private final FSNameSystem fsNameSystem;

    /**
     * 负管理集群中的所有数据节点
     */
    private final DataNodeManager dataNodeManager;

    /**
     * 是否停止运行
     */
    public static final AtomicBoolean SHUTDOWN = new AtomicBoolean(false);

    /**
     * 阻塞器
     */
    private final Lock LOCK = new ReentrantLock();
    private final Condition KEEPER = LOCK.newCondition();

    public NameNodeRpcServer() {
        this.fsNameSystem = new FSNameSystem();
        this.dataNodeManager = new DataNodeManager();
    }

    /**
     * 启动服务
     *
     * @throws IOException IO异常
     */
    public void start() throws IOException {
        // 启动RPC服务
        server = ServerBuilder
                .forPort(NAME_NODE_RPC_SERVER_PORT)
                .addService(new NameNodeService(fsNameSystem, dataNodeManager, this))
                .build()
                .start();

        logger.debug("name node rpc server start and listen {} port", server.getPort());

        // 注册回调钩子
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                logger.warn("*** shutting down {} since JVM is shutting down", NameNodeRpcServer.this.getClass().getSimpleName());
                NameNodeRpcServer.this.shutdown();
                logger.warn("*** {} shut down", NameNodeRpcServer.this.getClass().getSimpleName());
            }
        });
    }

    /**
     * 停止服务
     */
    public void shutdown() {
        if (!SHUTDOWN.compareAndSet(false, true)) {
            return;
        }

        if (server != null) {   // 停止RPC服务
            server.shutdown();
        }

        this.fsNameSystem.shutdown();   // 停止元数据管理组件

        leave();    // 唤醒等待服务
    }

    /**
     * 阻塞等待服务
     */
    public void keep() {
        LOCK.lock();
        try {
            KEEPER.await();
        } catch (InterruptedException ignore) {
        } finally {
            LOCK.unlock();
        }
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
}
