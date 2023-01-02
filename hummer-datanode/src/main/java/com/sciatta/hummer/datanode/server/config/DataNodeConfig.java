package com.sciatta.hummer.datanode.server.config;

import com.sciatta.hummer.core.config.ConfigManager;
import com.sciatta.hummer.core.util.PathUtils;

/**
 * Created by Rain on 2022/12/13<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 数据节点配置
 */
public class DataNodeConfig {
    private static final ConfigManager configManager = ConfigManager.getInstance();

    /**
     * 获取主元数据节点RPC主机名
     *
     * @return 主元数据节点RPC主机名
     */
    public static String getActiveNameNodeRpcHost() {
        return configManager.getStringConfig("ACTIVE_NAME_NODE_RPC_HOST", "localhost");
    }

    /**
     * 获取主元数据节点RPC端口
     *
     * @return 主元数据节点RPC端口
     */
    public static int getActiveNameNodeRpcPort() {
        return configManager.getIntConfig("ACTIVE_NAME_NODE_RPC_PORT", 3030);
    }

    /**
     * 获取本地主机名
     *
     * @return 本地主机名
     */
    public static String getLocalHostname() {
        return configManager.getStringConfig("LOCAL_HOST", "localhost");
    }

    /**
     * 获取本地主机端口
     *
     * @return 本地主机端口
     */
    public static int getLocalPort() {
        return configManager.getIntConfig("LOCAL_PORT", 9000);
    }

    /**
     * 获取文件服务处理线程数
     *
     * @return 文件服务处理线程数
     */
    public static int getFileServerWorkerCount() {
        return configManager.getIntConfig("FILE_SERVER_WORKER_COUNT", 3);
    }

    /**
     * 获取数据节点根目录
     *
     * @return 数据节点根目录
     */
    public static String getDataNodeRootPath() {
        return configManager.getStringConfig("DATA_NODE_ROOT_PATH",
                PathUtils.getPathWithSlashAtLast(PathUtils.getUserHome()) +
                        "hummer");
    }

    /**
     * 获取数据节点文件存储目录
     *
     * @return 数据节点文件存储目录
     */
    public static String getDataNodeDataPath() {
        return configManager.getStringConfig("DATA_NODE_DATA_PATH",
                PathUtils.getPathWithSlashAtLast(getDataNodeRootPath()) +
                        "datas" + PathUtils.getFileSeparator() +
                        getLocalHostname() + PathUtils.getFileSeparator());
    }
}
