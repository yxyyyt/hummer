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
        return configManager.getIntConfig("EDITS_LOG_BUFFER_LIMIT", 25 * 1024);
    }

    /**
     * 获取元数据节点根目录
     *
     * @return 元数据节点根目录
     */
    public static String getNameNodeRootPath() {
        return configManager.getStringConfig("NAME_NODE_ROOT_PATH",
                PathUtils.getPathWithSlashAtLast(PathUtils.getUserHome()) +
                        "hummer");
    }

    /**
     * 获取事务日志持久化路径
     *
     * @return 事务日志持久化路径
     */
    public static String getEditsLogPath() {
        return configManager.getStringConfig("EDITS_LOG_PATH",
                PathUtils.getPathWithSlashAtLast(getNameNodeRootPath()) +
                        "meta-data" + PathUtils.getFileSeparator() +
                        "namenode" + PathUtils.getFileSeparator() +
                        "editslog" + PathUtils.getFileSeparator());
    }

    /**
     * 获取检查点路径
     *
     * @return 检查点路径
     */
    public static String getCheckpointPath() {
        return configManager.getStringConfig("CHECKPOINT_PATH",
                PathUtils.getPathWithSlashAtLast(getNameNodeRootPath()) +
                        "meta-data" + PathUtils.getFileSeparator() +
                        "namenode" + PathUtils.getFileSeparator() +
                        "checkpoint" + PathUtils.getFileSeparator());
    }

    /**
     * 获取运行时仓库路径
     *
     * @return 运行时仓库路径
     */
    public static String getRuntimeRepositoryPath() {
        return configManager.getStringConfig("RUNTIME_REPOSITORY_PATH",
                PathUtils.getPathWithSlashAtLast(getNameNodeRootPath()) +
                        "meta-data" + PathUtils.getFileSeparator() +
                        "namenode" + PathUtils.getFileSeparator() +
                        "runtime" + PathUtils.getFileSeparator());
    }

    /**
     * 获取元数据节点RPC服务端口
     *
     * @return 元数据节点RPC服务端口
     */
    public static int getNameNodeRpcServerPort() {
        return configManager.getIntConfig("NAME_NODE_RPC_SERVER_PORT", 3030);
    }

    /**
     * 获取元数据节点镜像上传服务端口
     *
     * @return 元数据节点镜像上传服务端口
     */
    public static int getNameNodeImageUploadServerPort() {
        return configManager.getIntConfig("NAME_NODE_IMAGE_UPLOAD_SERVER_PORT", 4040);
    }

    /**
     * 获取备份节点向元数据节点抓取事务日志一个批次的最大大小
     *
     * @return 备份节点向元数据节点抓取事务日志一个批次的最大大小
     */
    public static int getBackupNodeMaxFetchSize() {
        return configManager.getIntConfig("BACKUP_NODE_MAX_FETCH_SIZE", 10);
    }

    /**
     * 获取数据文件副本的数量
     *
     * @return 数据文件副本的数量
     */
    public static int getNumberOfReplicated() {
        return configManager.getIntConfig("NUMBER_OF_REPLICATED", 2);
    }
}
