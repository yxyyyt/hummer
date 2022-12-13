package com.sciatta.hummer.namenode.server;

import com.sciatta.hummer.core.util.PathUtils;

import java.io.IOException;
import java.nio.file.Path;

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

    /**
     * 获取事务日志文件存储路径
     *
     * @param flushedMinTxId 刷写磁盘最小事务标识
     * @param flushedMaxTxId 刷写磁盘最大事务标识
     * @return 事务日志文件存储路径
     * @throws IOException IO异常
     */
    public static Path getEditsLogFile(long flushedMinTxId, long flushedMaxTxId) throws IOException {
        String file = EDITS_LOG_PATH + "edits-" + flushedMinTxId + "-" + flushedMaxTxId + ".log";
        return PathUtils.getPathAndCreateDirectoryIfNotExists(file);
    }
}
