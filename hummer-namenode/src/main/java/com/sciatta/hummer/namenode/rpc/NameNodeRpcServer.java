package com.sciatta.hummer.namenode.rpc;

import com.sciatta.hummer.namenode.config.NameNodeConfig;
import com.sciatta.hummer.namenode.fs.FSNameSystem;
import com.sciatta.hummer.core.server.Server;
import com.sciatta.hummer.namenode.data.DataNodeManager;
import io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by Rain on 2022/12/16<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 对外提供RPC服务，使用Grpc实现
 */
public class NameNodeRpcServer {
    private static final Logger logger = LoggerFactory.getLogger(NameNodeRpcServer.class);

    private final FSNameSystem fsNameSystem;
    private final DataNodeManager dataNodeManager;
    private final Server server;

    /**
     * grpc internal server
     */
    private io.grpc.Server grpcServer = null;

    public NameNodeRpcServer(FSNameSystem fsNameSystem, DataNodeManager dataNodeManager, Server server) {
        this.fsNameSystem = fsNameSystem;
        this.dataNodeManager = dataNodeManager;
        this.server = server;
    }

    /**
     * 启动RPC服务
     */
    public void start() throws IOException {
        grpcServer = ServerBuilder.forPort(NameNodeConfig.getNameNodeRpcServerPort())
                .addService(new NameNodeRpcService(fsNameSystem, dataNodeManager, server))
                .build()
                .start();

        logger.debug("name node rpc server start and listen {} port", grpcServer.getPort());
    }

    /**
     * 停止RPC服务
     */
    public void stop() {
        if (grpcServer != null) {
            grpcServer.shutdown();
        }
    }
}
