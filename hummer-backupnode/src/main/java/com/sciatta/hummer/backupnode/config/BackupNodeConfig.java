package com.sciatta.hummer.backupnode.config;

/**
 * Created by Rain on 2022/12/15<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 备份节点配置
 */
public class BackupNodeConfig {
    /**
     * 元数据节点RPC主机
     */
    public static final String NAME_NODE_RPC_HOST = "localhost";

    /**
     * 元数据节点RPC端口
     */
    public static final int NAME_NODE_RPC_PORT = 3030;

    /**
     * 向元数据节点抓取一个批次事务日志进行重放的限定数量
     */
    public static final int BACKUP_NODE_FETCH_SIZE = 10;

    /**
     * 磁盘同步最大内存缓存大小，单位：字节
     */
    public static final int EDITS_LOG_BUFFER_LIMIT = 25 * 1024;

    /**
     * 元数据节点根目录
     */
    public static final String BACKUP_NODE_ROOT_PATH = "D:\\data\\hummer\\data\\backupnode\\";

    /**
     * 事务日志持久化路径
     */
    public static final String EDITS_LOG_PATH = BACKUP_NODE_ROOT_PATH + "editslog\\";
}
