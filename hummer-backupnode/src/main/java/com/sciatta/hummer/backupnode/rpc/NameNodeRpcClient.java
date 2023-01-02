package com.sciatta.hummer.backupnode.rpc;

import com.google.gson.reflect.TypeToken;
import com.sciatta.hummer.backupnode.config.BackupNodeConfig;
import com.sciatta.hummer.core.fs.editlog.EditLog;
import com.sciatta.hummer.core.util.GsonUtils;
import com.sciatta.hummer.rpc.FetchEditsLogRequest;
import com.sciatta.hummer.rpc.FetchEditsLogResponse;
import com.sciatta.hummer.rpc.NameNodeServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Rain on 2022/12/15<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 元数据节点RPC客户端
 */
public class NameNodeRpcClient {
    private static final Logger logger = LoggerFactory.getLogger(NameNodeRpcClient.class);

    private final NameNodeServiceGrpc.NameNodeServiceBlockingStub nameNodeService;

    public NameNodeRpcClient() {
        ManagedChannel channel = ManagedChannelBuilder.forAddress(
                        BackupNodeConfig.getNameNodeRpcHost(), BackupNodeConfig.getNameNodeRpcPort())
                .usePlaintext()
                .build();

        this.nameNodeService = NameNodeServiceGrpc.newBlockingStub(channel);
    }

    /**
     * 从元数据节点获取未同步的事务日志
     *
     * @param syncedTxId 当前已同步到的事务标识
     * @return 未同步的事务日志列表
     */
    public List<EditLog> fetchEditsLog(long syncedTxId) {
        FetchEditsLogRequest request = FetchEditsLogRequest.newBuilder()
                .setCode(1)
                .setSyncedTxId(syncedTxId)
                .build();

        FetchEditsLogResponse response = nameNodeService.fetchEditsLog(request);
        if (response.getStatus() != 1) {    // TODO 统一响应状态
            logger.debug("fetch editLog response status is " + response.getStatus());
            return new ArrayList<>();
        }

        String json = response.getEditsLog();

        List<EditLog> editsLog = GsonUtils.fromJson(json, new TypeToken<List<EditLog>>() {
        }.getType());

        logger.debug("fetch editsLog size is " + editsLog.size());

        return editsLog;
    }
}
