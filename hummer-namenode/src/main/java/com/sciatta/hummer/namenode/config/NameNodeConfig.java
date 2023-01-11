package com.sciatta.hummer.namenode.config;

import com.sciatta.hummer.core.config.ConfigManager;
import com.sciatta.hummer.core.util.PathUtils;

/**
 * Created by Rain on 2022/12/7<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 元数据节点配置
 */
public final class NameNodeConfig {

    private static final ConfigManager configManager = ConfigManager.getInstance();

    /**
     * 获取磁盘同步最大内存缓存大小，单位：字节
     *
     * @return 磁盘同步最大内存缓存大小，单位：字节
     */
    public static int getEditsLogBufferLimit() {
        return configManager.getIntConfig("edits.log.buffer.limit", 25 * 1024);
    }

    /**
     * 获取元数据节点根目录
     *
     * @return 元数据节点根目录
     */
    public static String getNameNodeRootPath() {
        return configManager.getStringConfig("name.node.root.path",
                PathUtils.getPathWithSlashAtLast(PathUtils.getUserHome()) +
                        "hummer");
    }

    /**
     * 获取元数据节点元数据目录
     *
     * @return 元数据节点元数据目录
     */
    public static String getNameNodeMetaDataPath() {
        return configManager.getStringConfig("name.node.meta.data.path",
                PathUtils.getPathWithSlashAtLast(getNameNodeRootPath()) +
                        "meta-data" + PathUtils.getFileSeparator() +
                        "namenode");
    }

    /**
     * 获取事务日志持久化路径
     *
     * @return 事务日志持久化路径
     */
    public static String getEditsLogPath() {
        return configManager.getStringConfig("edits.log.path",
                PathUtils.getPathWithSlashAtLast(getNameNodeMetaDataPath()) + "editslog");
    }

    /**
     * 获取检查点路径
     *
     * @return 检查点路径
     */
    public static String getCheckpointPath() {
        return configManager.getStringConfig("checkpoint.path",
                PathUtils.getPathWithSlashAtLast(getNameNodeMetaDataPath()) + "checkpoint");
    }

    /**
     * 获取运行时仓库路径
     *
     * @return 运行时仓库路径
     */
    public static String getRuntimeRepositoryPath() {
        return configManager.getStringConfig("runtime.repository.path",
                PathUtils.getPathWithSlashAtLast(getNameNodeMetaDataPath()) + "runtime");
    }

    /**
     * 获取元数据节点RPC服务端口
     *
     * @return 元数据节点RPC服务端口
     */
    public static int getNameNodeRpcServerPort() {
        return configManager.getIntConfig("name.node.rpc.server.port", 3030);
    }

    /**
     * 获取元数据节点镜像上传服务端口
     *
     * @return 元数据节点镜像上传服务端口
     */
    public static int getNameNodeImageUploadServerPort() {
        return configManager.getIntConfig("name.node.image.upload.server.port", 4040);
    }

    /**
     * 获取备份节点向元数据节点抓取事务日志一个批次的最大大小
     *
     * @return 备份节点向元数据节点抓取事务日志一个批次的最大大小
     */
    public static int getBackupNodeMaxFetchSize() {
        return configManager.getIntConfig("backup.node.max.fetch.size", 10);
    }

    /**
     * 获取数据文件副本的数量
     *
     * @return 数据文件副本的数量
     */
    public static int getNumberOfReplicated() {
        return configManager.getIntConfig("number.of.replicated", 2);
    }

    /**
     * 获取数据节点存活状态检查监控间隔
     *
     * @return 数据节点存活状态检查监控间隔
     */
    public static int getDataNodeAliveMonitorInterval() {
        return configManager.getIntConfig("data.node.alive.monitor.interval", 30 * 1000);
    }

    /**
     * 获取不可用数据节点心跳间隔
     *
     * @return 不可用数据节点心跳间隔
     */
    public static int getUnavailableDataNodeHeartbeatInterval() {
        return configManager.getIntConfig("unavailable.data.node.heartbeat.interval", 90 * 1000);
    }
}
