package com.sciatta.hummer.namenode.config;

/**
 * Created by Rain on 2022/12/7<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 元数据节点配置
 */
public class NameNodeConfig {
    /**
     * 磁盘同步最大内存缓存大小，单位：字节
     */
    public static final int EDITS_LOG_BUFFER_LIMIT = 25 * 1024;

    /**
     * 元数据节点根目录
     */
    public static final String NAME_NODE_ROOT_PATH = "D:\\data\\hummer\\namenode\\";

    /**
     * 事务日志持久化路径
     */
    public static final String EDITS_LOG_PATH = NAME_NODE_ROOT_PATH + "editslog\\";

    /**
     * 元数据节点RPC服务端口号
     */
    public static final int NAME_NODE_RPC_SERVER_PORT = 3030;
}
