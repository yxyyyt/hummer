package com.sciatta.hummer.client.rpc;

import com.google.gson.reflect.TypeToken;
import com.sciatta.hummer.client.config.FileSystemClientConfig;
import com.sciatta.hummer.core.data.DataNodeInfo;
import com.sciatta.hummer.core.transport.TransportStatus;
import com.sciatta.hummer.core.util.GsonUtils;
import com.sciatta.hummer.rpc.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by Rain on 2022/12/16<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 元数据节点RPC客户端
 */
public class NameNodeRpcClient {

    private static final Logger logger = LoggerFactory.getLogger(NameNodeRpcClient.class);

    private final NameNodeServiceGrpc.NameNodeServiceBlockingStub nameNodeServiceGrpc;

    public NameNodeRpcClient() {
        ManagedChannel channel = ManagedChannelBuilder.forAddress(
                        FileSystemClientConfig.getNameNodeRpcHost(), FileSystemClientConfig.getNameNodeRpcPort())
                .usePlaintext()
                .build();

        this.nameNodeServiceGrpc = NameNodeServiceGrpc.newBlockingStub(channel);
    }

    /**
     * 创建目录
     *
     * @param path 目录路径
     * @return 创建目录响应
     */
    public int mkdir(String path) {
        MkdirRequest request = MkdirRequest.newBuilder().setPath(path).build();
        MkdirResponse response = nameNodeServiceGrpc.mkdir(request);

        logger.debug("mkdir response status is " + response.getStatus());

        return response.getStatus();
    }

    /**
     * 创建文件
     *
     * @param fileName 文件名
     * @return 创建文件响应
     */
    public int createFile(String fileName) {
        CreateFileRequest request = CreateFileRequest.newBuilder().setFileName(fileName).build();
        CreateFileResponse response = nameNodeServiceGrpc.createFile(request);

        logger.debug("create file response status is " + response.getStatus());

        return response.getStatus();
    }

    /**
     * 分配数据节点
     *
     * @param fileName 文件名
     * @param fileSize 文件大小
     * @return 已成功分配的数据节点；若没有分配成功，则返回null
     */
    public List<DataNodeInfo> allocateDataNodes(String fileName, long fileSize) {
        AllocateDataNodesRequest request = AllocateDataNodesRequest.newBuilder()
                .setFileName(fileName)
                .setFileSize(fileSize)
                .build();
        AllocateDataNodesResponse response = nameNodeServiceGrpc.allocateDataNodes(request);

        logger.debug("allocate dataNodes response status is " + response.getStatus());

        if (response.getStatus() == TransportStatus.AllocateDataNodes.SUCCESS) {
            return GsonUtils.fromJson(response.getDataNodes(), new TypeToken<List<DataNodeInfo>>() {
            }.getType());
        }

        return null;
    }


    /**
     * 优雅停机
     *
     * @return 停机是否成功
     */
    public int shutdown() {
        ShutdownRequest request = ShutdownRequest.newBuilder().setCode(1).build();
        ShutdownResponse response = nameNodeServiceGrpc.shutdown(request);

        logger.debug("shutdown response status is " + response.getStatus());

        return response.getStatus();
    }
}
