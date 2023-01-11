package com.sciatta.hummer.datanode.config;

import com.sciatta.hummer.core.config.ConfigManager;
import com.sciatta.hummer.core.exception.HummerException;
import com.sciatta.hummer.core.util.PathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Rain on 2022/12/13<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 数据节点配置
 */
public class DataNodeConfig {
    private static final Logger logger = LoggerFactory.getLogger(DataNodeConfig.class);

    private static final ConfigManager configManager = ConfigManager.getInstance();

    /**
     * 单节点
     */
    private static final String RUN_MODE_STAND_ALONE = "stand-alone";

    /**
     * 集群
     */
    private static final String RUN_MODE_CLUSTER = "cluster";

    /**
     * 单节点集群
     */
    private static final String RUN_MODE_SINGLE_NODE_CLUSTER = "single-node-cluster";

    /**
     * 获取运行模式
     *
     * <pre>
     * stand-alone：单节点
     * cluster：集群
     * single-node-cluster：单节点集群
     * </pre>
     *
     * @return 运行模式
     */
    public static String getRunMode() {
        String runMode = configManager.getStringConfig("run.mode", RUN_MODE_STAND_ALONE);

        if (RUN_MODE_STAND_ALONE.equals(runMode) ||
                RUN_MODE_CLUSTER.equals(runMode) ||
                RUN_MODE_SINGLE_NODE_CLUSTER.equals(runMode)) {
            return runMode;
        }

        logger.debug("{} not supported, support ( {} | {} | {} ) only",
                runMode, RUN_MODE_STAND_ALONE, RUN_MODE_CLUSTER, RUN_MODE_SINGLE_NODE_CLUSTER);
        throw new HummerException("%s not supported, support ( %s | %s | %s ) only",
                runMode, RUN_MODE_STAND_ALONE, RUN_MODE_CLUSTER, RUN_MODE_SINGLE_NODE_CLUSTER);
    }

    /**
     * 获取本地主机名
     *
     * @return 本地主机名
     */
    public static String getLocalHostname() {
        return configManager.getStringConfig("local.host", "localhost");
    }

    /**
     * 获取本地主机端口
     *
     * @return 本地主机端口
     */
    public static int getLocalPort() {
        return configManager.getIntConfig("local.port", 9000);
    }

    /**
     * 获取文件服务处理线程数
     *
     * @return 文件服务处理线程数
     */
    public static int getFileServerWorkerCount() {
        return configManager.getIntConfig("file.server.worker.count", 3);
    }

    /**
     * 获取数据节点根目录
     *
     * @return 数据节点根目录
     */
    public static String getDataNodeRootPath() {
        return configManager.getStringConfig("data.node.root.path",
                PathUtils.getPathWithSlashAtLast(PathUtils.getUserHome()) + "hummer");
    }

    /**
     * 获取数据节点文件存储目录
     *
     * @return 数据节点文件存储目录
     */
    public static String getDataNodeDataPath() {
        String dataPath = configManager.getStringConfig("data.node.data.path",
                PathUtils.getPathWithSlashAtLast(getDataNodeRootPath()) + "data");

        if (getRunMode().equals(RUN_MODE_SINGLE_NODE_CLUSTER)) {
            dataPath = PathUtils.getPathWithSlashAtLast(dataPath) +
                    getLocalHostname() + PathUtils.getFileSeparator() + getLocalPort();
        }

        return dataPath;
    }

    /**
     * 获取心跳间隔
     *
     * @return 心跳间隔
     */
    public static int getHeartbeatInterval() {
        return configManager.getIntConfig("heartbeat.interval", 30 * 1000);
    }
}
