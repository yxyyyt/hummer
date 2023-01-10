package com.sciatta.hummer.backupnode.config;

import com.sciatta.hummer.core.config.ConfigManager;
import com.sciatta.hummer.core.util.PathUtils;

/**
 * Created by Rain on 2022/12/15<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 备份节点配置
 */
public class BackupNodeConfig {
    private static final ConfigManager configManager = ConfigManager.getInstance();

    /**
     * 获取元数据节点镜像上传主机
     *
     * @return 元数据节点镜像上传主机
     */
    public static String getNameNodeImageUploadHost() {
        return configManager.getStringConfig("NAME_NODE_IMAGE_UPLOAD_HOST", "localhost");
    }

    /**
     * 获取元数据节点镜像上传端口
     *
     * @return 元数据节点镜像上传端口
     */
    public static int getNameNodeImageUploadPort() {
        return configManager.getIntConfig("NAME_NODE_IMAGE_UPLOAD_PORT", 4040);
    }

    /**
     * 获取向元数据节点抓取一个批次事务日志进行重放的限定数量
     *
     * @return 向元数据节点抓取一个批次事务日志进行重放的限定数量
     */
    public static int getBackupNodeFetchSize() {
        return configManager.getIntConfig("BACKUP_NODE_FETCH_SIZE", 10);
    }

    /**
     * 获取磁盘同步最大内存缓存大小，单位：字节
     *
     * @return 磁盘同步最大内存缓存大小，单位：字节
     */
    public static int getEditsLogBufferLimit() {
        return configManager.getIntConfig("EDITS_LOG_BUFFER_LIMIT", 25 * 1024);
    }

    /**
     * 获取备份节点根目录
     *
     * @return 备份节点根目录
     */
    public static String getBackupNodeRootPath() {
        return configManager.getStringConfig("BACKUP_NODE_ROOT_PATH",
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
                PathUtils.getPathWithSlashAtLast(getBackupNodeRootPath()) +
                        "meta-data" + PathUtils.getFileSeparator() +
                        "backupnode" + PathUtils.getFileSeparator() +
                        "editslog" + PathUtils.getFileSeparator());
    }

    /**
     * 获取检查点路径
     *
     * @return 检查点路径
     */
    public static String getCheckpointPath() {
        return configManager.getStringConfig("CHECKPOINT_PATH",
                PathUtils.getPathWithSlashAtLast(getBackupNodeRootPath()) +
                        "meta-data" + PathUtils.getFileSeparator() +
                        "backupnode" + PathUtils.getFileSeparator() +
                        "checkpoint" + PathUtils.getFileSeparator());
    }

    /**
     * 获取运行时仓库路径
     *
     * @return 运行时仓库路径
     */
    public static String getRuntimeRepositoryPath() {
        return configManager.getStringConfig("RUNTIME_REPOSITORY_PATH",
                PathUtils.getPathWithSlashAtLast(getBackupNodeRootPath()) +
                        "meta-data" + PathUtils.getFileSeparator() +
                        "backupnode" + PathUtils.getFileSeparator() +
                        "runtime" + PathUtils.getFileSeparator());
    }

    /**
     * 获取检查点定时执行间隔
     *
     * @return 检查点定时执行间隔
     */
    public static int getCheckpointInterval() {
        return configManager.getIntConfig("CHECKPOINT_INTERVAL", 60 * 1000);
    }
}
