package com.sciatta.hummer.client.rpc;

import com.google.gson.reflect.TypeToken;
import com.sciatta.hummer.client.config.ClientConfig;
import com.sciatta.hummer.core.fs.data.DataNodeInfo;
import com.sciatta.hummer.core.fs.data.StorageInfo;
import com.sciatta.hummer.core.fs.editlog.EditLog;
import com.sciatta.hummer.core.server.Holder;
import com.sciatta.hummer.core.transport.TransportStatus;
import com.sciatta.hummer.core.transport.command.Command;
import com.sciatta.hummer.core.transport.command.RemoteCommand;
import com.sciatta.hummer.core.util.GsonUtils;
import com.sciatta.hummer.rpc.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
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
                        ClientConfig.getNameNodeRpcHost(), ClientConfig.getNameNodeRpcPort())
                .usePlaintext()
                .build();

        this.nameNodeServiceGrpc = NameNodeServiceGrpc.newBlockingStub(channel);
    }

    /**
     * 向元数据节点发送注册请求
     *
     * @param dataNodeHostname 数据节点主机名
     * @param dataNodePort     数据节点端口
     * @return 注册响应状态
     */
    public int register(String dataNodeHostname, int dataNodePort) {
        try {
            RegisterRequest request = RegisterRequest.newBuilder()
                    .setHostname(dataNodeHostname)
                    .setPort(dataNodePort)
                    .build();

            RegisterResponse response = nameNodeServiceGrpc.register(request);

            logger.debug("data node {}:{} register to name node, response status is {}",
                    dataNodeHostname, dataNodePort, response.getStatus());

            return response.getStatus();
        } catch (Throwable e) {
            logger.error("{} while data node {}:{} register to name node",
                    e.getMessage(), dataNodeHostname, dataNodePort);
        }

        return TransportStatus.Register.FAIL;
    }

    /**
     * 向元数据节点发送心跳请求
     *
     * @param dataNodeHostname 数据节点主机名
     * @param dataNodePort     数据节点端口
     * @param holder           持有远程命名集合
     * @return 心跳响应状态
     */
    public int heartbeat(String dataNodeHostname, int dataNodePort, Holder<List<Command>> holder) {
        try {
            HeartbeatRequest request = HeartbeatRequest.newBuilder()
                    .setHostname(dataNodeHostname)
                    .setPort(dataNodePort)
                    .build();

            HeartbeatResponse response = nameNodeServiceGrpc.heartbeat(request);

            logger.debug("data node {}:{} heartbeat to name node, response status is {}, remote command is {}",
                    dataNodeHostname, dataNodePort, response.getStatus(), response.getRemoteCommand());

            holder.set(GsonUtils.fromJson(response.getRemoteCommand(), RemoteCommand.class).getCommands());

            return response.getStatus();
        } catch (Throwable e) {
            logger.error("{} while data node {}:{} heartbeat to name node",
                    e.getMessage(), dataNodeHostname, dataNodePort);
        }

        return TransportStatus.HeartBeat.FAIL;
    }

    /**
     * 向元数据节点增量上报文件存储信息
     *
     * @param dataNodeHostname 数据节点主机名
     * @param dataNodePort     数据节点端口
     * @param fileName         文件名
     * @param fileSize         文件大小
     * @return 增量上报文件存储信息响应状态
     */
    public int incrementalReport(String dataNodeHostname, int dataNodePort, String fileName, long fileSize) {
        try {
            IncrementalReportRequest request = IncrementalReportRequest.newBuilder()
                    .setHostname(dataNodeHostname)
                    .setPort(dataNodePort)
                    .setFileName(fileName)
                    .setFileSize(fileSize)
                    .build();

            IncrementalReportResponse response = nameNodeServiceGrpc.incrementalReport(request);

            logger.debug("data node {}:{} incremental report file name {}, size {} to name node, response status is {}",
                    dataNodeHostname, dataNodePort, fileName, fileSize, response.getStatus());

            return response.getStatus();
        } catch (Throwable e) {
            logger.error("{} while data node {}:{} incremental report file name {}, size {} to name node",
                    e.getMessage(), dataNodeHostname, dataNodePort, fileName, fileSize);
        }

        return TransportStatus.IncrementalReport.FAIL;
    }

    /**
     * 向元数据节点全量上报文件存储信息
     *
     * @param dataNodeHostname 数据节点主机名
     * @param dataNodePort     数据节点端口
     * @param storageInfo      存储信息
     * @return 全量上报文件存储信息响应状态
     */
    public int fullReport(String dataNodeHostname, int dataNodePort, StorageInfo storageInfo) {
        try {
            FullReportRequest request = FullReportRequest.newBuilder()
                    .setHostname(dataNodeHostname)
                    .setPort(dataNodePort)
                    .setFileNames(GsonUtils.toJson(storageInfo.getFileNames()))
                    .setFileSizes(GsonUtils.toJson(storageInfo.getFileSizes()))
                    .build();

            FullReportResponse response = nameNodeServiceGrpc.fullReport(request);

            logger.debug("data node {}:{} full report file names {}, sizes {} to name node, response status is {}",
                    dataNodeHostname, dataNodePort, storageInfo.getFileNames(), storageInfo.getFileSizes(), response.getStatus());

            return response.getStatus();
        } catch (Throwable e) {
            logger.error("{} while data node {}:{} full report file names {}, sizes {} to name node",
                    e.getMessage(), dataNodeHostname, dataNodePort, storageInfo.getFileNames(), storageInfo.getFileSizes());
        }

        return TransportStatus.FullReport.FAIL;
    }

    /**
     * 创建目录
     *
     * @param path 目录路径
     * @return 创建目录响应状态
     */
    public int mkdir(String path) {
        try {
            MkdirRequest request = MkdirRequest.newBuilder().setPath(path).build();

            MkdirResponse response = nameNodeServiceGrpc.mkdir(request);

            logger.debug("mkdir {}, response status is {}", path, response.getStatus());

            return response.getStatus();
        } catch (Throwable e) {
            logger.error("{} while mkdir {}", e.getMessage(), path);
        }

        return TransportStatus.Mkdir.FAIL;
    }

    /**
     * 创建文件
     *
     * @param fileName 文件名
     * @return 创建文件响应状态
     */
    public int createFile(String fileName) {
        try {
            CreateFileRequest request = CreateFileRequest.newBuilder().setFileName(fileName).build();

            CreateFileResponse response = nameNodeServiceGrpc.createFile(request);

            logger.debug("create file {}, response status is {}", fileName, response.getStatus());

            return response.getStatus();
        } catch (Throwable e) {
            logger.error("{} while create file {}", e.getMessage(), fileName);
        }

        return TransportStatus.CreateFile.FAIL;
    }

    /**
     * 分配数据节点
     *
     * @param fileName 文件名
     * @param holder   持有已成功分配的数据节点；若没有分配成功，则持有null
     * @return 分配数据节点响应状态
     */
    public int allocateDataNodes(String fileName, Holder<List<DataNodeInfo>> holder) {
        try {
            AllocateDataNodesRequest request = AllocateDataNodesRequest.newBuilder()
                    .setFileName(fileName)
                    .build();

            AllocateDataNodesResponse response = nameNodeServiceGrpc.allocateDataNodes(request);

            if (response.getStatus() == TransportStatus.AllocateDataNodes.SUCCESS) {
                holder.set(GsonUtils.fromJson(response.getDataNodes(), new TypeToken<List<DataNodeInfo>>() {
                }.getType()));
            }

            logger.debug("allocate data node {} for file name {}, response status is {}",
                    holder.get(), fileName, response.getStatus());

            return response.getStatus();
        } catch (Throwable e) {
            logger.error("{} while allocate data node for file name {}", e.getMessage(), fileName);
        }

        return TransportStatus.AllocateDataNodes.FAIL;
    }

    /**
     * 获得存储文件所在的数据节点
     *
     * @param fileName 文件名
     * @param holder   持有存储文件所在的数据节点；若不存在，则持有null
     * @return 获得存储文件所在的数据节点响应状态
     */
    public int getDataNodeForFile(String fileName, Holder<DataNodeInfo> holder) {
        try {
            GetDataNodeForFileRequest request = GetDataNodeForFileRequest.newBuilder()
                    .setFileName(fileName)
                    .build();

            GetDataNodeForFileResponse response = nameNodeServiceGrpc.getDataNodeForFile(request);

            if (response.getStatus() == TransportStatus.GetDataNodeForFile.SUCCESS) {
                holder.set(GsonUtils.fromJson(response.getDataNodeInfo(), DataNodeInfo.class));
            }

            logger.debug("get data node {} for file name {}, response status is {}",
                    holder.get(), fileName, response.getStatus());

            return response.getStatus();
        } catch (Throwable e) {
            logger.error("{} while get data node for file name {}", e.getMessage(), fileName);
        }

        return TransportStatus.GetDataNodeForFile.FAIL;
    }

    /**
     * 从元数据节点获取未同步的事务日志
     *
     * @param syncedTxId 当前已同步到的事务标识
     * @return 未同步的事务日志列表
     */
    public int fetchEditsLog(long syncedTxId, Holder<List<EditLog>> holder) {
        FetchEditsLogRequest request = FetchEditsLogRequest.newBuilder()
                .setCode(1)
                .setSyncedTxId(syncedTxId)
                .build();

        FetchEditsLogResponse response = nameNodeServiceGrpc.fetchEditsLog(request);

        if (response.getStatus() == TransportStatus.FetchEditsLog.SUCCESS) {
            List<EditLog> editsLog = GsonUtils.fromJson(response.getEditsLog(), new TypeToken<List<EditLog>>() {
            }.getType());
            holder.set(editsLog);
        }

        logger.debug("last synced txId {}, fetch editsLog response edits log {}, status is {}",
                syncedTxId, holder.get(), response.getStatus());

        return response.getStatus();
    }

    /**
     * 优雅停机
     *
     * @return 停机是否成功
     */
    public int shutdown() {
        try {
            ShutdownRequest request = ShutdownRequest.newBuilder().setCode(1).build();

            ShutdownResponse response = nameNodeServiceGrpc.shutdown(request);

            logger.debug("shutdown response status is {}", response.getStatus());

            return response.getStatus();
        } catch (Throwable e) {
            logger.error("{} while shutdown", e.getMessage());
        }

        return TransportStatus.Shutdown.FAIL;
    }
}
