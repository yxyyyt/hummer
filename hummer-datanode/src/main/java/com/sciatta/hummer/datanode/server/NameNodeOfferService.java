package com.sciatta.hummer.datanode.server;

import com.sciatta.hummer.datanode.server.config.DataNodeConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

/**
 * Created by Rain on 2022/12/13<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 同一组元数据节点通信
 */
public class NameNodeOfferService {
    private static final Logger logger = LoggerFactory.getLogger(NameNodeOfferService.class);

    // private final NameNodeServiceActor standbyServiceActor; TODO standby

    private final CopyOnWriteArrayList<NameNodeServiceActor> serviceActors = new CopyOnWriteArrayList<>();

    public NameNodeOfferService() {
        NameNodeServiceActor activeServiceActor = new NameNodeServiceActor(
                DataNodeConfig.getActiveNameNodeRpcHost(), DataNodeConfig.getActiveNameNodeRpcPort());

        this.serviceActors.add(activeServiceActor);
    }

    /**
     * 启动
     */
    public void start() {
        register();
        heartbeat();
    }

    /**
     * 向元数据节点发起注册
     */
    private void register() {
        // 异步向一组NameNode发起注册
        CountDownLatch latch = new CountDownLatch(serviceActors.size());

        for (NameNodeServiceActor nameNodeServiceActor : serviceActors) {
            nameNodeServiceActor.register(latch);
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("{} exception while register", e.getMessage());
        }

        logger.info("all name node have been registered");
    }

    /**
     * 向元数据节点发送心跳
     */
    private void heartbeat() {
        for (NameNodeServiceActor nameNodeServiceActor : serviceActors) {
            nameNodeServiceActor.heartbeat();
        }
    }
}
