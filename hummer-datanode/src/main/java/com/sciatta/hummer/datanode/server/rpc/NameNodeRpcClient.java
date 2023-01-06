package com.sciatta.hummer.datanode.server.rpc;

import com.google.gson.reflect.TypeToken;
import com.sciatta.hummer.core.transport.Command;
import com.sciatta.hummer.core.transport.TransportStatus;
import com.sciatta.hummer.core.util.GsonUtils;
import com.sciatta.hummer.datanode.server.config.DataNodeConfig;
import com.sciatta.hummer.datanode.server.fs.StorageInfo;
import com.sciatta.hummer.rpc.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.omg.CORBA.TRANSACTION_MODE;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by Rain on 2023/1/2<br>
 * All Rights Reserved(C) 2017 - 2023 SCIATTA <br> <p/>
 * 元数据节点RPC客户端
 */
public class NameNodeRpcClient {
    private static final Logger logger = LoggerFactory.getLogger(NameNodeRpcClient.class);

    private final NameNodeServiceGrpc.NameNodeServiceBlockingStub nameNodeServiceGrpc;

    public NameNodeRpcClient() {
        ManagedChannel channel = ManagedChannelBuilder.forAddress(
                        DataNodeConfig.getNameNodeRpcHost(), DataNodeConfig.getNameNodeRpcPort())
                .usePlaintext()
                .build();

        this.nameNodeServiceGrpc = NameNodeServiceGrpc.newBlockingStub(channel);
    }

    /**
     * 向元数据节点发送注册请求
     *
     * @return 注册响应状态
     */
    public int register() {
        logger.debug("send request to name node for registration");

        RegisterRequest request = RegisterRequest.newBuilder()
                .setHostname(DataNodeConfig.getLocalHostname())
                .setPort(DataNodeConfig.getLocalPort())
                .build();

        RegisterResponse response = nameNodeServiceGrpc.register(request);
        logger.debug("receive registration response from name node, status is {}", response.getStatus());

        return response.getStatus();
    }

    /**
     * 向元数据节点发送心跳请求
     *
     * @param commands 命名集合
     * @return 心跳响应状态
     */
    public int heartbeat(List<Command> commands) {
        logger.debug("send request to name node for heartbeat");

        HeartbeatRequest request = HeartbeatRequest.newBuilder()
                .setHostname(DataNodeConfig.getLocalHostname())
                .setPort(DataNodeConfig.getLocalPort())
                .build();

        try {
            HeartbeatResponse response = nameNodeServiceGrpc.heartbeat(request);
            commands.addAll(GsonUtils.fromJson(response.getRemoteCommands(), new TypeToken<List<Command>>() {
            }.getType()));

            logger.debug("receive heartbeat response from name node, status is {}, commands is {}",
                    response.getStatus(), commands);

            return response.getStatus();
        } catch (Throwable e) {
            logger.error("{} while heartbeat to name node", e.getMessage());
            return TransportStatus.HeartBeat.FAIL;
        }
    }


    /**
     * 向元数据节点增量上报文件存储信息
     *
     * @param fileName 文件名
     */
    public void incrementalReport(String fileName) {
        logger.debug("send file name {} to name node for incremental report", fileName);

        IncrementalReportRequest request = IncrementalReportRequest.newBuilder()
                .setHostname(DataNodeConfig.getLocalHostname())
                .setPort(DataNodeConfig.getLocalPort())
                .setFileName(fileName)
                .build();

        IncrementalReportResponse response = nameNodeServiceGrpc.incrementalReport(request);

        logger.debug("receive incremental report response from name node, status is {}", response.getStatus());
    }

    /**
     * 向元数据节点全量上报文件存储信息
     *
     * @param storageInfo 存储信息
     * @return 全量上报文件存储信息响应状态
     */
    public int fullReport(StorageInfo storageInfo) {
        logger.debug("send storage info {} to name node for full report", storageInfo);

        FullReportRequest request = FullReportRequest.newBuilder()
                .setHostname(DataNodeConfig.getLocalHostname())
                .setPort(DataNodeConfig.getLocalPort())
                .setFileNames(GsonUtils.toJson(storageInfo.getFileNames()))
                .setStoredDataSize(storageInfo.getStoredDataSize())
                .build();

        FullReportResponse response = nameNodeServiceGrpc.fullReport(request);
        logger.debug("receive full report response from name node, status is {}", response.getStatus());

        return response.getStatus();
    }
}
