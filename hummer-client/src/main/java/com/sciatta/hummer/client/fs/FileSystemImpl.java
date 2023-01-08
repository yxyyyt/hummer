package com.sciatta.hummer.client.fs;

import com.sciatta.hummer.client.rpc.NameNodeRpcClient;
import com.sciatta.hummer.core.data.DataNodeInfo;
import com.sciatta.hummer.core.transport.TransportStatus;

import java.util.List;

/**
 * Created by Rain on 2022/10/15<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 文件系统客户端实现
 */
public class FileSystemImpl implements FileSystem {
    private final NameNodeRpcClient nameNodeRpcClient;
    private final DataNodeFileClient dataNodeFileClient;

    public FileSystemImpl() {
        this.nameNodeRpcClient = new NameNodeRpcClient();
        this.dataNodeFileClient = new DataNodeFileClient();
    }

    @Override
    public int mkdir(String path) {
        return this.nameNodeRpcClient.mkdir(path);
    }

    @Override
    public boolean uploadFile(byte[] file, String fileName, long fileSize) {
        // 向元数据节点创建文件元数据
        if (this.nameNodeRpcClient.createFile(fileName) != TransportStatus.CreateFile.SUCCESS) {
            return false;
        }

        // 向元数据节点申请分配数据节点
        List<DataNodeInfo> dataNodes = this.nameNodeRpcClient.allocateDataNodes(fileName, fileSize);
        if (dataNodes == null || dataNodes.size() <= 0) {
            return false;
        }

        // 向各个数据节点上传文件
        for (DataNodeInfo dataNode : dataNodes) {
            dataNodeFileClient.uploadFile(dataNode.getHostname(), dataNode.getPort(), file, fileName, fileSize);
        }

        return true;
    }

    @Override
    public byte[] downloadFile(String filename) {
        DataNodeInfo dataNode = this.nameNodeRpcClient.getDataNodeForFile(filename);

        if (dataNode == null) {
            return null;
        }

        return this.dataNodeFileClient.downloadFile(dataNode.getHostname(), dataNode.getPort(), filename);
    }

    @Override
    public int shutdown() {
        return this.nameNodeRpcClient.shutdown();
    }
}
