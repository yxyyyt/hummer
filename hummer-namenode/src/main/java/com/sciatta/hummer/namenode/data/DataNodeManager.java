package com.sciatta.hummer.namenode.data;

import com.sciatta.hummer.core.data.DataNodeInfo;
import com.sciatta.hummer.core.server.Server;
import com.sciatta.hummer.namenode.data.allocate.DataNodeAllocator;
import com.sciatta.hummer.namenode.data.allocate.StoredDataSizeAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Rain on 2022/12/13<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 管理集群中的所有数据节点
 */
public class DataNodeManager {
    private static final Logger logger = LoggerFactory.getLogger(DataNodeManager.class);

    /**
     * 数据节点缓存
     */
    private final Map<String, DataNodeInfo> aliveDataNodes = new ConcurrentHashMap<>();

    private final DataNodeAliveMonitor dataNodeAliveMonitor;
    private final DataNodeAllocator dataNodeAllocator;

    private final Server server;

    public DataNodeManager(Server server) {
        // 启动数据节点存活状态检查监控线程
        this.dataNodeAliveMonitor = new DataNodeAliveMonitor();
        this.dataNodeAllocator = new StoredDataSizeAllocator();
        this.server = server;
    }

    public void start() {
        this.dataNodeAliveMonitor.start();
    }

    /**
     * 数据节点发起注册
     *
     * @param ip                   IP地址
     * @param hostname             主机名
     * @param fileUploadServerPort 文件上传服务端口
     * @return 是否注册成功；true，注册成功；否则，注册失败
     */
    public boolean register(String ip, String hostname, int fileUploadServerPort) {
        DataNodeInfo datanode = new DataNodeInfo(ip, hostname, fileUploadServerPort);
        aliveDataNodes.put(DataNodeInfo.uniqueKey(ip, hostname), datanode);
        logger.debug("data node {}({}) register success", hostname, ip);
        return true;
    }

    /**
     * 数据节点发起心跳
     *
     * @param ip       IP地址
     * @param hostname 主机名
     * @return 是否心跳成功；true，心跳成功；否则，心跳失败
     */
    public boolean heartbeat(String ip, String hostname) {
        DataNodeInfo datanode = aliveDataNodes.get(DataNodeInfo.uniqueKey(ip, hostname));
        if (datanode == null) {
            return false;
        }

        datanode.setLatestHeartbeatTime(System.currentTimeMillis());
        logger.debug("data node {}({}) heartbeat success", hostname, ip);
        return true;
    }

    /**
     * 分配数据节点
     *
     * @param fileSize 数据文件大小
     * @return 已成功分配的数据节点
     */
    public List<DataNodeInfo> allocateDataNodes(long fileSize) {
        List<DataNodeInfo> dataNodes = this.dataNodeAllocator.allocateDataNodes(this.aliveDataNodes);

        dataNodes.forEach(d -> d.addStoredDataSize(fileSize));

        return Collections.unmodifiableList(dataNodes);
    }

    /**
     * 数据节点存活状态检查监控线程
     */
    private class DataNodeAliveMonitor extends Thread { // TODO to 线程管理组件

        @Override
        public void run() {
            try {
                while (!server.isClosing()) {
                    List<String> toRemoveDataNodes = new ArrayList<>();

                    Iterator<DataNodeInfo> dataNodeInfoIterator = aliveDataNodes.values().iterator();
                    DataNodeInfo datanode;
                    while (dataNodeInfoIterator.hasNext()) {
                        datanode = dataNodeInfoIterator.next();
                        if (System.currentTimeMillis() - datanode.getLatestHeartbeatTime() > 90 * 1000) {   // TODO to config
                            toRemoveDataNodes.add(DataNodeInfo.uniqueKey(datanode.getIp(), datanode.getHostname()));
                        }
                    }

                    if (!toRemoveDataNodes.isEmpty()) {
                        for (String toRemoveDatanode : toRemoveDataNodes) {
                            aliveDataNodes.remove(toRemoveDatanode);
                        }
                    }

                    Thread.sleep(30 * 1000);    // TODO to config
                }
            } catch (Exception e) {
                e.printStackTrace();    // TODO log and exception
            }
        }

    }
}
