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
    private final FSNameSystem fsNameSystem;

    /**
     * 负管理集群中的所有数据节点
     */
    private final DataNodeManager dataNodeManager;

    /**
     * 元数据节点RPC服务端
     */
    private final NameNodeRpcServer nameNodeRpcServer;

    public NameNodeService(FSNameSystem fsNameSystem, DataNodeManager dataNodeManager, NameNodeRpcServer nameNodeRpcServer) {
        this.fsNameSystem = fsNameSystem;
        this.dataNodeManager = dataNodeManager;
        this.nameNodeRpcServer = nameNodeRpcServer;
    }

    @Override   // TODO 重构 请求处理分发器
    public void register(RegisterRequest request, StreamObserver<RegisterResponse> responseObserver) {
        RegisterResponse response;

        if (NameNodeRpcServer.SHUTDOWN.get()) {
            response = RegisterResponse.newBuilder().setStatus(STATUS_FAILURE).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            return;
        }

        boolean test = dataNodeManager.register(request.getIp(), request.getHostname());

        if (test) {
            response = RegisterResponse.newBuilder().setStatus(STATUS_SUCCESS).build();
        } else {
            response = RegisterResponse.newBuilder().setStatus(STATUS_FAILURE).build();
        }

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void heartbeat(HeartbeatRequest request, StreamObserver<HeartbeatResponse> responseObserver) {
        HeartbeatResponse response;

        if (NameNodeRpcServer.SHUTDOWN.get()) {
            response = HeartbeatResponse.newBuilder().setStatus(STATUS_FAILURE).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            return;
        }

        boolean test = dataNodeManager.heartbeat(request.getIp(), request.getHostname());

        if (test) {
            response = HeartbeatResponse.newBuilder().setStatus(STATUS_SUCCESS).build();
        } else {
            response = HeartbeatResponse.newBuilder().setStatus(STATUS_FAILURE).build();
        }

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void mkdir(MkdirRequest request, StreamObserver<MkdirResponse> responseObserver) {
        MkdirResponse response;

        if (NameNodeRpcServer.SHUTDOWN.get()) {
            response = MkdirResponse.newBuilder().setStatus(STATUS_FAILURE).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            return;
        }

        boolean test = fsNameSystem.mkdir(request.getPath());

        if (test) {
            response = MkdirResponse.newBuilder().setStatus(STATUS_SUCCESS).build();
        } else {
            response = MkdirResponse.newBuilder().setStatus(STATUS_FAILURE).build();
        }

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void shutdown(ShutdownRequest request, StreamObserver<ShutdownResponse> responseObserver) {
        ShutdownResponse response;

        if (NameNodeRpcServer.SHUTDOWN.get()) {
            response = ShutdownResponse.newBuilder().setStatus(STATUS_FAILURE).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            return;
        }

        nameNodeRpcServer.shutdown();

        response = ShutdownResponse.newBuilder().setStatus(STATUS_SUCCESS).build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
