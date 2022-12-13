package com.sciatta.hummer.namenode.server;

import com.sciatta.hummer.rpc.*;
import io.grpc.stub.StreamObserver;

/**
 * Created by Rain on 2022/12/13<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 元数据节点对外提供RPC服务接口
 */
public class NameNodeService extends NameNodeServiceGrpc.NameNodeServiceImplBase {

    public static final Integer STATUS_SUCCESS = 1;
    public static final Integer STATUS_FAILURE = 2;

    /**
     * 管理元数据
     */
    private FSNameSystem fsNameSystem;

    /**
     * 负管理集群中的所有数据节点
     */
    private DataNodeManager dataNodeManager;

    public NameNodeService(FSNameSystem fsNameSystem, DataNodeManager dataNodeManager) {
        this.fsNameSystem = fsNameSystem;
        this.dataNodeManager = dataNodeManager;
    }

    @Override
    public void register(RegisterRequest request, StreamObserver<RegisterResponse> responseObserver) {
        dataNodeManager.register(request.getIp(), request.getHostname());       // TODO 重构 请求处理分发器

        RegisterResponse response = RegisterResponse.newBuilder().setStatus(STATUS_SUCCESS).build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void heartbeat(HeartbeatRequest request, StreamObserver<HeartbeatResponse> responseObserver) {
        dataNodeManager.heartbeat(request.getIp(), request.getHostname());

        HeartbeatResponse response = HeartbeatResponse.newBuilder().setStatus(STATUS_SUCCESS).build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
