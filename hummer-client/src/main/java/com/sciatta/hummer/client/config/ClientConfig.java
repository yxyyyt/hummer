package com.sciatta.hummer.client.config;

import com.sciatta.hummer.core.config.ConfigManager;

/**
 * Created by Rain on 2022/12/13<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 文件系统客户端配置
 */
public class ClientConfig {
    private static final ConfigManager configManager = ConfigManager.getInstance();

    /**
     * 获取元数据节点RPC主机
     *
     * @return 元数据节点RPC主机
     */
    public static String getNameNodeRpcHost() {
        return configManager.getStringConfig("name.node.rpc.host", "localhost");
    }

    /**
     * 获取元数据节点RPC端口
     *
     * @return 元数据节点RPC端口
     */
    public static int getNameNodeRpcPort() {
        return configManager.getIntConfig("name.node.rpc.port", 3030);
    }
}
