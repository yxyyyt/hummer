package com.sciatta.hummer.namenode.server;

import com.sciatta.hummer.rpc.NameNodeServiceGrpc;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

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
    private FSNameSystem fsNameSystem;

    /**
     * 负管理集群中的所有数据节点
     */
    private DataNodeManager dataNodeManager;

    public NameNodeRpcServer(FSNameSystem fsNameSystem, DataNodeManager dataNodeManager) {
        this.fsNameSystem = fsNameSystem;
        this.dataNodeManager = dataNodeManager;
    }

    public void start() throws IOException {
        // 启动一个rpc server，监听指定的端口号
        // 同时绑定好了自己开发的接口
        server = ServerBuilder
                .forPort(NAME_NODE_RPC_SERVER_PORT)
                .addService(new NameNodeService(fsNameSystem, dataNodeManager))
                .build()
                .start();

        logger.debug("name node rpc server start and listen {} port", server.getPort());

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                NameNodeRpcServer.this.stop();
            }
        });
    }

    public void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }
}
