package com.sciatta.hummer.client.fs;

import com.sciatta.hummer.client.rpc.NameNodeRpcClient;
import com.sciatta.hummer.core.fs.data.DataNodeInfo;
import com.sciatta.hummer.core.server.Holder;
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
        Holder<List<DataNodeInfo>> holder = new Holder<>();
        int status = this.nameNodeRpcClient.allocateDataNodes(fileName, holder);
        if (status != TransportStatus.AllocateDataNodes.SUCCESS) {
            return false;
        }

        List<DataNodeInfo> dataNodeInfos = holder.get();

        if (dataNodeInfos == null || dataNodeInfos.size() <= 0) {
            return false;
        }

        // 向各个数据节点上传文件
        for (DataNodeInfo dataNodeInfo : dataNodeInfos) {
            dataNodeFileClient.uploadFile(dataNodeInfo.getHostname(), dataNodeInfo.getPort(), file, fileName, fileSize);
        }

        return true;
    }

    @Override
    public byte[] downloadFile(String filename) {
        Holder<DataNodeInfo> holder = new Holder<>();
        int status = this.nameNodeRpcClient.getDataNodeForFile(filename, holder);

        if (status != TransportStatus.GetDataNodeForFile.SUCCESS || holder.get() == null) {
            return null;
        }

        return this.dataNodeFileClient.downloadFile(holder.get().getHostname(), holder.get().getPort(), filename);
    }

    @Override
    public int shutdown() {
        return this.nameNodeRpcClient.shutdown();
    }
}
