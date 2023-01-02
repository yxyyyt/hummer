package com.sciatta.hummer.client.fs;

import com.sciatta.hummer.client.rpc.NameNodeRpcClient;
import com.sciatta.hummer.core.data.DataNodeInfo;

import java.util.List;

/**
 * Created by Rain on 2022/10/15<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 文件系统客户端实现
 */
public class FileSystemImpl implements FileSystem {
    private final NameNodeRpcClient client;

    public FileSystemImpl() {
        this.client = new NameNodeRpcClient();
    }

    @Override
    public int mkdir(String path) {
        return this.client.mkdir(path);
    }

    @Override
    public boolean uploadFile(byte[] file, String fileName, long fileSize) {
        // 向元数据节点创建文件元数据
        if (this.client.createFile(fileName) != 1) {    // TODO 通信代码
            return false;
        }

        // 向元数据节点申请分配数据节点
        List<DataNodeInfo> dataNodes = this.client.allocateDataNodes(fileName, fileSize);
        if (dataNodes == null || dataNodes.size() <= 0) {
            return false;
        }

        // 向各个数据节点上传文件
        for (DataNodeInfo dataNode : dataNodes) {
            DataNodeFileClient.sendFile(dataNode.getHostname(), dataNode.getPort(), file, fileName, fileSize);
        }

        return true;
    }

    @Override
    public int shutdown() {
        return this.client.shutdown();
    }
}
