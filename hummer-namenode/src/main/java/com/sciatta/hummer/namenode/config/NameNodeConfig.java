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
    public static final String NAME_NODE_ROOT_PATH = "D:\\data\\hummer\\meta-data\\namenode\\";

    /**
     * 事务日志持久化路径
     */
    public static final String EDITS_LOG_PATH = NAME_NODE_ROOT_PATH + "editslog\\";

    /**
     * 检查点路径
     */
    public static final String CHECKPOINT_PATH = NAME_NODE_ROOT_PATH + "checkpoint\\";

    /**
     * 运行时仓库路径
     */
    public static final String RUNTIME_REPOSITORY_PATH = NAME_NODE_ROOT_PATH+ "runtime\\";

    /**
     * 元数据节点RPC服务端口
     */
    public static final int NAME_NODE_RPC_SERVER_PORT = 3030;

    /**
     * 元数据节点镜像上传服务端口
     */
    public static final int NAME_NODE_IMAGE_UPLOAD_SERVER_PORT = 4040;

    /**
     * 备份节点向元数据节点抓取事务日志一个批次的最大大小
     */
    public static final int BACKUP_NODE_MAX_FETCH_SIZE = 10;

    /**
     * 数据文件副本的数量
     */
    public static final int NUMBER_OF_REPLICATED = 2;
}
