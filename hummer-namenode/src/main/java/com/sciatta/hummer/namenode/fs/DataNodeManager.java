package com.sciatta.hummer.namenode.fs;

import com.sciatta.hummer.core.fs.DataNodeInfo;
import com.sciatta.hummer.core.server.Server;
import com.sciatta.hummer.core.transport.TransportStatus;
import com.sciatta.hummer.namenode.NameNode;
import com.sciatta.hummer.namenode.config.NameNodeConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;

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

    /**
     * 数据节点上的所有文件
     */
    private final ConcurrentMap<String, Set<String>> dataNodeToFileNamesCache = new ConcurrentHashMap<>();

    /**
     * 数据节点对应的文件复制任务
     */
    private final ConcurrentMap<String, Set<ReplicateTask>> dataNodeToReplicateTasks = new ConcurrentHashMap<>();

    /**
     * 数据节点对应的文件删除任务
     */
    private final ConcurrentMap<String, Set<String>> dataNodeToRemoveReplicaTasks = new ConcurrentHashMap<>();

    private final DataNodeAliveMonitor dataNodeAliveMonitor;
    private final DataNodeAllocator dataNodeAllocator;

    private final Server server;

    public DataNodeManager(Server server) {
        // 启动数据节点存活状态检查监控线程
        this.dataNodeAliveMonitor = new DataNodeAliveMonitor();
        this.dataNodeAllocator = new DataNodeAllocator(this);
        this.server = server;
    }

    public Map<String, DataNodeInfo> getAvailableDataNodes() {
        return availableDataNodes;
    }

    public ConcurrentMap<String, Set<DataNodeInfo>> getFileNameToDataNodeCache() {
        return fileNameToDataNodeCache;
    }

    public ConcurrentMap<String, Set<ReplicateTask>> getDataNodeToReplicateTasks() {
        return dataNodeToReplicateTasks;
    }

    public ConcurrentMap<String, Set<String>> getDataNodeToRemoveReplicaTasks() {
        return dataNodeToRemoveReplicaTasks;
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
            dataNode.setLatestHeartbeatTime(System.currentTimeMillis());
            availableDataNodes.put(uniqueKey, dataNode);
            logger.debug("data node {}:{} has registered, but not available, update register time {}", hostname,
                    port, dataNode.getLatestRegisterTime());
            return TransportStatus.Register.SUCCESS;
        }

        // 新注册
        dataNode = new DataNodeInfo(hostname, port);
        dataNode.setLatestRegisterTime(System.currentTimeMillis());
        dataNode.setLatestHeartbeatTime(System.currentTimeMillis());
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
     * 为上传文件分配可用数据节点
     *
     * @return 已成功分配的数据节点
     */
    public List<DataNodeInfo> allocateDataNodes() {
        List<DataNodeInfo> dataNodes = this.dataNodeAllocator.allocateDataNodes();

        return dataNodes == null ? null : Collections.unmodifiableList(dataNodes);
    }

    /**
     * 获取文件所在数据节点的其中一个可用数据节点
     *
     * @param fileName 文件名
     * @return 文件所在数据节点的其中一个可用数据节点
     */
    public DataNodeInfo selectOneDataNodeForFile(String fileName) {
        return this.dataNodeAllocator.selectOneDataNodeForFile(fileName);
    }

    /**
     * 数据节点文件增量上报
     *
     * @param hostname 数据节点主机名
     * @param port     数据节点端口
     * @param fileName 文件名
     * @param fileSize 文件大小
     * @return 是否增量上报成功；true，增量上报成功；否则，增量上报失败
     */
    public boolean incrementalReport(String hostname, int port, String fileName, long fileSize) {
        DataNodeInfo dataNode = this.availableDataNodes.get(DataNodeInfo.uniqueKey(hostname, port));
        if (dataNode == null) return false;

        addCache(fileName, fileSize, dataNode);

        return true;
    }

    /**
     * 数据节点文件全量上报
     *
     * @param hostname  数据节点主机名
     * @param port      数据节点端口
     * @param fileNames 数据节点上的所有文件名
     * @param fileSizes 数据节点上的所有文件大小
     * @return 是否全量上报成功；true，全量上报成功；否则，全量上报失败
     */
    public boolean fullReport(String hostname, int port, List<String> fileNames, List<Long> fileSizes) {
        DataNodeInfo dataNode = this.availableDataNodes.get(DataNodeInfo.uniqueKey(hostname, port));
        if (dataNode == null) return false;

        for (int i = 0; i < fileNames.size(); i++) {
            addCache(fileNames.get(i), fileSizes.get(i), dataNode);
        }

        return true;
    }

    /**
     * 设置缓存
     *
     * @param fileName     文件名
     * @param fileSize     文件大小
     * @param dataNodeInfo 文件对应的副本所在的数据节点
     */
    private void addCache(String fileName, long fileSize, DataNodeInfo dataNodeInfo) {
        String uniqueKey = DataNodeInfo.uniqueKey(dataNodeInfo.getHostname(),
                dataNodeInfo.getPort());

        // 每个文件对应的副本所在的数据节点
        Set<DataNodeInfo> dataNodeInfos = this.fileNameToDataNodeCache.get(fileName);
        if (dataNodeInfos == null) {
            this.fileNameToDataNodeCache.putIfAbsent(fileName, new CopyOnWriteArraySet<>());
            dataNodeInfos = this.fileNameToDataNodeCache.get(fileName);
        }

        // 冗余的文件上报，生成删除任务
        if (dataNodeInfos.size() == NameNodeConfig.getNumberOfReplicated()) {
            Set<String> removeReplicaTasks = dataNodeToRemoveReplicaTasks.get(uniqueKey);
            if (removeReplicaTasks == null) {
                this.dataNodeToRemoveReplicaTasks.putIfAbsent(uniqueKey, new CopyOnWriteArraySet<>());
                removeReplicaTasks = this.dataNodeToRemoveReplicaTasks.get(uniqueKey);
            }
            removeReplicaTasks.add(fileName);
            return;
        }

        dataNodeInfos.add(dataNodeInfo);

        // 数据节点上的所有文件
        Set<String> fileNames = this.dataNodeToFileNamesCache.get(uniqueKey);
        if (fileNames == null) {
            this.dataNodeToFileNamesCache.putIfAbsent(uniqueKey, new CopyOnWriteArraySet<>());
            fileNames = this.dataNodeToFileNamesCache.get(uniqueKey);
        }
        fileNames.add(fileName);

        // 累加已存储数据的大小
        dataNodeInfo.addDataSize(fileSize);
    }

    /**
     * 清空不可用数据节点缓存
     *
     * @param unavailableDataNode 不可用数据节点
     */
    private void clearCache(DataNodeInfo unavailableDataNode) {
        String uniqueKey = DataNodeInfo.uniqueKey(unavailableDataNode.getHostname(), unavailableDataNode.getPort());
        Set<String> fileNames = this.dataNodeToFileNamesCache.get(uniqueKey);

        for (String fileName : fileNames) {
            Set<DataNodeInfo> dataNodeInfos = this.fileNameToDataNodeCache.get(fileName);
            if (dataNodeInfos != null) {
                dataNodeInfos.remove(unavailableDataNode);
            }
        }

        this.dataNodeToFileNamesCache.remove(uniqueKey);
    }

    /**
     * 创建副本复制任务
     *
     * @param unavailableDataNode 不可用数据节点
     * @return 为不可用数据节点创建的复制任务数量
     */
    private int buildReplicateTask(DataNodeInfo unavailableDataNode) {
        String uniqueKey = DataNodeInfo.uniqueKey(unavailableDataNode.getHostname(), unavailableDataNode.getPort());
        Set<String> fileNames = this.dataNodeToFileNamesCache.get(uniqueKey);

        int replicateTaskNum = 0;
        for (String fileName : fileNames) {
            DataNodeInfo source = this.dataNodeAllocator.allocatorReplicateSource(fileName, unavailableDataNode);
            DataNodeInfo dest = this.dataNodeAllocator.allocatorReplicateDestination(fileName);

            if (source != null && dest != null & !source.equals(dest)) {
                String destUniqueKey = DataNodeInfo.uniqueKey(dest.getHostname(), dest.getPort());
                Set<ReplicateTask> replicateTasks = this.dataNodeToReplicateTasks.get(destUniqueKey);
                if (replicateTasks == null) {
                    this.dataNodeToReplicateTasks.putIfAbsent(destUniqueKey, new CopyOnWriteArraySet<>());
                    replicateTasks = this.dataNodeToReplicateTasks.get(destUniqueKey);
                }
                replicateTasks.add(new ReplicateTask(fileName, source, dest));
                replicateTaskNum++;
                logger.debug("add replicate task, file name {}, source data node {}, dest data node {}",
                        fileName, source, dest);
            } else {
                logger.warn("no replication task added, hotspot data may appear, file name {}, source data node {}, dest data node {}",
                        fileName, source, dest);
            }
        }

        return replicateTaskNum;
    }

    /**
     * 数据节点存活状态检查监控线程
     */
    private class DataNodeAliveMonitor extends Thread { // TODO to 线程管理组件
        private final Logger logger = LoggerFactory.getLogger(DataNodeAliveMonitor.class);

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

                            // 从可用节点转移到不可用节点
                            unavailableDataNodes.put(uniqueKey, availableDataNodes.remove(uniqueKey));
                            logger.debug("{} removed from available data node", toRemovedDataNode);

                            // 创建复制文件任务
                            int replicateTaskNum = buildReplicateTask(toRemovedDataNode);
                            logger.debug("build {} replicate tasks for {}", replicateTaskNum, toRemovedDataNode);

                            // 清空缓存
                            clearCache(toRemovedDataNode);
                            logger.debug("clear {} cache finish", toRemovedDataNode);
                        }
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
