package com.sciatta.hummer.namenode.data;

import com.sciatta.hummer.core.data.DataNodeInfo;
import com.sciatta.hummer.core.server.Server;
import com.sciatta.hummer.core.transport.TransportStatus;
import com.sciatta.hummer.namenode.config.NameNodeConfig;
import com.sciatta.hummer.namenode.data.allocate.DataNodeAllocator;
import com.sciatta.hummer.namenode.data.allocate.impl.StoredDataSizeAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by Rain on 2022/12/13<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 管理集群中的所有数据节点
 */
public class DataNodeManager {
    private static final Logger logger = LoggerFactory.getLogger(DataNodeManager.class);

    /**
     * 可用数据节点缓存
     */
    private final Map<String, DataNodeInfo> availableDataNodes = new ConcurrentHashMap<>();

    /**
     * 不可用数据节点缓存
     */
    private final Map<String, DataNodeInfo> unavailableDataNodes = new ConcurrentHashMap<>();

    /**
     * 每个文件对应的副本所在的数据节点
     */
    private final ConcurrentMap<String, Set<DataNodeInfo>> fileNameToDataNodeCache = new ConcurrentHashMap<>();

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
     * @param hostname 数据节点主机名
     * @param port     数据节点端口
     * @return 注册状态 {@link TransportStatus.Register}
     */
    public int register(String hostname, int port) {
        String uniqueKey = DataNodeInfo.uniqueKey(hostname, port);

        DataNodeInfo dataNode = availableDataNodes.get(uniqueKey);
        // 已注册且可用
        if (dataNode != null) {
            logger.debug("data node {}:{} has registered and available, latest register time {}", hostname,
                    port, dataNode.getLatestRegisterTime());
            return TransportStatus.Register.REGISTERED;
        }

        dataNode = unavailableDataNodes.get(uniqueKey);
        // 已注册且不可用
        if (dataNode != null) {
            unavailableDataNodes.remove(uniqueKey);
            dataNode.setLatestRegisterTime(System.currentTimeMillis());
            availableDataNodes.put(uniqueKey, dataNode);
            logger.debug("data node {}:{} has registered, but not available, update register time {}", hostname,
                    port, dataNode.getLatestRegisterTime());
            return TransportStatus.Register.SUCCESS;
        }

        // 新注册
        dataNode = new DataNodeInfo(hostname, port);
        dataNode.setLatestRegisterTime(System.currentTimeMillis());
        availableDataNodes.put(uniqueKey, dataNode);
        logger.debug("data node {}:{} register success, register time {}", hostname, port,
                dataNode.getLatestRegisterTime());
        return TransportStatus.Register.SUCCESS;
    }

    /**
     * 数据节点发起心跳
     *
     * @param hostname 数据节点主机名
     * @param port     数据节点端口
     * @return 心跳状态 {@link TransportStatus.HeartBeat}
     */
    public int heartbeat(String hostname, int port) {
        String uniqueKey = DataNodeInfo.uniqueKey(hostname, port);

        DataNodeInfo dataNode = availableDataNodes.get(uniqueKey);
        // 已注册且可用
        if (dataNode != null) {
            dataNode.setLatestHeartbeatTime(System.currentTimeMillis());
            logger.debug("data node {}:{} has registered and available, update heartbeat time {}", hostname,
                    port, dataNode.getLatestHeartbeatTime());
            return TransportStatus.Register.SUCCESS;
        }

        dataNode = unavailableDataNodes.get(uniqueKey);
        // 已注册且不可用
        if (dataNode != null) {
            logger.debug("data node {}:{} has registered, but not available, latest heartbeat time {}", hostname,
                    port, dataNode.getLatestHeartbeatTime());
            return TransportStatus.HeartBeat.NOT_REGISTERED;
        }

        // 未注册
        logger.debug("data node {}:{} has not registered", hostname, port);
        return TransportStatus.HeartBeat.NOT_REGISTERED;
    }

    /**
     * 分配数据节点
     *
     * @param fileSize 数据文件大小
     * @return 已成功分配的数据节点
     */
    public List<DataNodeInfo> allocateDataNodes(long fileSize) {
        List<DataNodeInfo> dataNodes = this.dataNodeAllocator.allocateDataNodes(this.availableDataNodes);

        dataNodes.forEach(d -> d.addStoredDataSize(fileSize));

        return Collections.unmodifiableList(dataNodes);
    }

    /**
     * 数据节点文件增量上报
     *
     * @param hostname 数据节点主机名
     * @param port     数据节点端口
     * @param fileName 文件名
     * @return 是否增量上报成功；true，增量上报成功；否则，增量上报失败
     */
    public boolean incrementalReport(String hostname, int port, String fileName) {
        DataNodeInfo dataNode = this.availableDataNodes.get(DataNodeInfo.uniqueKey(hostname, port));
        if (dataNode == null) return false;

        Set<DataNodeInfo> dataNodes = fileNameToDataNodeCache.get(fileName);
        if (dataNodes == null) {
            fileNameToDataNodeCache.putIfAbsent(fileName, new HashSet<>());
            dataNodes = fileNameToDataNodeCache.get(fileName);
        }

        synchronized (Objects.requireNonNull(dataNodes)) {
            dataNodes.add(dataNode);
        }

        return true;
    }

    /**
     * 数据节点文件全量上报
     *
     * @param hostname       数据节点主机名
     * @param port           数据节点端口
     * @param fileNames      数据节点上的所有文件名
     * @param storedDataSize 已存储数据的大小
     * @return 是否全量上报成功；true，全量上报成功；否则，全量上报失败
     */
    public boolean fullReport(String hostname, int port, List<String> fileNames, long storedDataSize) {
        DataNodeInfo dataNode = this.availableDataNodes.get(DataNodeInfo.uniqueKey(hostname, port));
        if (dataNode == null) return false;

        Map<String, Set<DataNodeInfo>> temp = new HashMap<>();
        for (String fileName : fileNames) {
            Set<DataNodeInfo> dataNodes = temp.get(fileName);
            if (dataNodes == null) {
                temp.putIfAbsent(fileName, new HashSet<>());
                dataNodes = temp.get(fileName);
            }
            dataNodes.add(dataNode);
        }
        fileNameToDataNodeCache.putAll(temp);

        dataNode.setStoredDataSize(storedDataSize);

        return true;
    }

    /**
     * 数据节点存活状态检查监控线程
     */
    private class DataNodeAliveMonitor extends Thread { // TODO to 线程管理组件

        @Override
        public void run() {
            try {
                while (!server.isClosing()) {
                    List<DataNodeInfo> toRemovedDataNodes = new ArrayList<>();
                    for (DataNodeInfo dataNode : availableDataNodes.values()) {
                        if (System.currentTimeMillis() - dataNode.getLatestHeartbeatTime() >
                                NameNodeConfig.getUnavailableDataNodeHeartbeatInterval()) {
                            toRemovedDataNodes.add(dataNode);
                        }
                    }

                    if (!toRemovedDataNodes.isEmpty()) {
                        for (DataNodeInfo toRemovedDataNode : toRemovedDataNodes) {
                            String uniqueKey = DataNodeInfo.uniqueKey(toRemovedDataNode.getHostname(), toRemovedDataNode.getPort());
                            unavailableDataNodes.put(uniqueKey, availableDataNodes.remove(uniqueKey));
                        }
                        logger.debug("{} removed from available data node cache", toRemovedDataNodes);
                    }

                    logger.debug("current available data node {}, current unAvailable data node {}",
                            availableDataNodes, unavailableDataNodes);

                    Thread.sleep(NameNodeConfig.getDataNodeAliveMonitorInterval());
                }
            } catch (Exception e) {
                logger.error("{} while data node alive monitor run", e.getMessage());
            }
        }

    }
}
