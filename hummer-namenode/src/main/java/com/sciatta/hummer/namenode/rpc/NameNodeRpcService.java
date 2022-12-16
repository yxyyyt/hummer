package com.sciatta.hummer.namenode.rpc;

import com.sciatta.hummer.core.fs.FSNameSystem;
import com.sciatta.hummer.core.server.Server;
import com.sciatta.hummer.namenode.datanode.DataNodeManager;
import com.sciatta.hummer.rpc.*;
import io.grpc.stub.StreamObserver;

/**
 * Created by Rain on 2022/12/13<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 元数据节点对外提供RPC服务接口，使用Grpc实现
 */
public class NameNodeRpcService extends NameNodeServiceGrpc.NameNodeServiceImplBase {

    /**
     * 成功
     */
    public static final Integer STATUS_SUCCESS = 1; // TODO to 通信常量

    /**
     * 失败
     */
    public static final Integer STATUS_FAILURE = 2;

    private final FSNameSystem fsNameSystem;
    private final DataNodeManager dataNodeManager;
    private final Server server;

    public NameNodeRpcService(FSNameSystem fsNameSystem, DataNodeManager dataNodeManager, Server server) {
        this.fsNameSystem = fsNameSystem;
        this.dataNodeManager = dataNodeManager;
        this.server = server;
    }

    @Override   // TODO 重构 请求处理分发器
    public void register(RegisterRequest request, StreamObserver<RegisterResponse> responseObserver) {
        RegisterResponse response;

        if (!server.isStarted() || server.isClosing()) {
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

        if (!server.isStarted() || server.isClosing()) {
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

        if (!server.isStarted() || server.isClosing()) {
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

        if (!server.isStarted() || server.isClosing()) {
            response = ShutdownResponse.newBuilder().setStatus(STATUS_FAILURE).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            return;
        }

        server.close();

        response = ShutdownResponse.newBuilder().setStatus(STATUS_SUCCESS).build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
