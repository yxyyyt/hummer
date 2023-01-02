package com.sciatta.hummer.datanode.server;

import com.sciatta.hummer.datanode.server.config.DataNodeConfig;
import com.sciatta.hummer.rpc.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

/**
 * Created by Rain on 2022/12/13<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 一组元数据节点中的一个角色，如主元数据节点，或备元数据节点，负责同特定元数据节点通信
 */
public class NameNodeServiceActor {
    private static final Logger logger = LoggerFactory.getLogger(NameNodeServiceActor.class);

    private final NameNodeServiceGrpc.NameNodeServiceBlockingStub nameNodeServiceGrpc;


    public NameNodeServiceActor(String host, int port) {
        ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();

        this.nameNodeServiceGrpc = NameNodeServiceGrpc.newBlockingStub(channel);
    }

    /**
     * 启动线程向元数据节点注册
     *
     * @param latch 同步注册门栓
     */
    public void register(CountDownLatch latch) {
        Thread registerThread = new RegisterThread(latch);
        registerThread.start();
    }

    /**
     * 启动线程向元数据节点发送心跳
     */
    public void heartbeat() {
        Thread heartbeatThread = new HeartbeatThread();
        heartbeatThread.start();
    }

    /**
     * 向元数据节点发起注册的线程
     */
    class RegisterThread extends Thread {

        private final CountDownLatch latch;

        public RegisterThread(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void run() {
            try {
                logger.debug("send request to name node for registration");

                RegisterRequest request = RegisterRequest.newBuilder()
                        .setHostname(DataNodeConfig.getLocalHostname())
                        .setPort(DataNodeConfig.getLocalPort())
                        .build();

                RegisterResponse response = nameNodeServiceGrpc.register(request);

                logger.debug("receive registration response from name node, status is " + response.getStatus());

                latch.countDown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 向元数据节点发送心跳的线程
     */
    class HeartbeatThread extends Thread {

        @Override
        public void run() {
            while (true) {
                logger.debug("send request to name node for heartbeat");

                HeartbeatRequest request = HeartbeatRequest.newBuilder()
                        .setHostname(DataNodeConfig.getLocalHostname())
                        .setPort(DataNodeConfig.getLocalPort())
                        .build();

                HeartbeatResponse response = nameNodeServiceGrpc.heartbeat(request);

                logger.debug("receive heartbeat response from name node, status is " + response.getStatus());

                try {
                    Thread.sleep(30 * 1000); // TODO 每隔30S发送心跳 to config
                } catch (InterruptedException e) {
                    logger.error("{} exception while wait to send heartbeat request", e.getMessage());
                }
            }
        }
    }
}
