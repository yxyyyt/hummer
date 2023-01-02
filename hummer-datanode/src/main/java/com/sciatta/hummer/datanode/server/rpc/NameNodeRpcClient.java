package com.sciatta.hummer.datanode.server.rpc;

import com.sciatta.hummer.core.server.Server;
import com.sciatta.hummer.datanode.server.config.DataNodeConfig;
import com.sciatta.hummer.rpc.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Rain on 2023/1/2<br>
 * All Rights Reserved(C) 2017 - 2023 SCIATTA <br> <p/>
 * 元数据节点RPC客户端
 */
public class NameNodeRpcClient {
    private static final Logger logger = LoggerFactory.getLogger(NameNodeRpcClient.class);

    private final NameNodeServiceGrpc.NameNodeServiceBlockingStub nameNodeServiceGrpc;
    private final Server server;

    public NameNodeRpcClient(Server server) {
        ManagedChannel channel = ManagedChannelBuilder.forAddress(
                        DataNodeConfig.getNameNodeRpcHost(), DataNodeConfig.getNameNodeRpcPort())
                .usePlaintext()
                .build();

        this.nameNodeServiceGrpc = NameNodeServiceGrpc.newBlockingStub(channel);
        this.server = server;
    }

    /**
     * 向元数据节点发起注册
     */
    public void heartbeat() {
        HeartbeatThread heartbeatThread = new HeartbeatThread();
        heartbeatThread.start();
    }

    /**
     * 向元数据节点发送心跳
     */
    public void register() throws InterruptedException {
        RegisterThread registerThread = new RegisterThread();
        registerThread.start();
        registerThread.join();
    }

    /**
     * 向元数据节点增量上报文件存储信息
     *
     * @param fileName 文件名
     */
    public void incrementalReport(String fileName) {
        logger.debug("send request to name node for incremental report");

        IncrementalReportRequest request = IncrementalReportRequest.newBuilder()
                .setHostname(DataNodeConfig.getLocalHostname())
                .setPort(DataNodeConfig.getLocalPort())
                .setFileName(fileName)
                .build();

        IncrementalReportResponse response = nameNodeServiceGrpc.incrementalReport(request);

        logger.debug("receive incremental report response from name node, status is {}", response.getStatus());
    }

    /**
     * 向元数据节点发起注册的线程
     */
    private class RegisterThread extends Thread {
        @Override
        public void run() {
            logger.debug("send request to name node for registration");

            RegisterRequest request = RegisterRequest.newBuilder()
                    .setHostname(DataNodeConfig.getLocalHostname())
                    .setPort(DataNodeConfig.getLocalPort())
                    .build();

            RegisterResponse response = nameNodeServiceGrpc.register(request);

            logger.debug("receive registration response from name node, status is {}", response.getStatus());
        }
    }

    /**
     * 向元数据节点发送心跳的线程
     */
    private class HeartbeatThread extends Thread {

        @Override
        public void run() {
            while (!server.isClosing()) {
                logger.debug("send request to name node for heartbeat");

                HeartbeatRequest request = HeartbeatRequest.newBuilder()
                        .setHostname(DataNodeConfig.getLocalHostname())
                        .setPort(DataNodeConfig.getLocalPort())
                        .build();

                HeartbeatResponse response = nameNodeServiceGrpc.heartbeat(request);

                logger.debug("receive heartbeat response from name node, status is {}", response.getStatus());

                try {
                    Thread.sleep(DataNodeConfig.getHeartbeatInterval());
                } catch (InterruptedException e) {
                    logger.error("{} exception while wait to send heartbeat request", e.getMessage());
                }
            }
        }
    }
}
