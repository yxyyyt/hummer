package com.sciatta.hummer.client.rpc;

import com.sciatta.hummer.client.FileSystem;
import com.sciatta.hummer.rpc.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.sciatta.hummer.client.config.FileSystemClientConfig.NAME_NODE_RPC_HOST;
import static com.sciatta.hummer.client.config.FileSystemClientConfig.NAME_NODE_RPC_PORT;

/**
 * Created by Rain on 2022/12/16<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 元数据节点RPC客户端
 */
public class NameNodeRpcClient implements FileSystem {

    private static final Logger logger = LoggerFactory.getLogger(NameNodeRpcClient.class);

    private final NameNodeServiceGrpc.NameNodeServiceBlockingStub nameNodeServiceGrpc;

    public NameNodeRpcClient() {
        ManagedChannel channel = ManagedChannelBuilder.forAddress(NAME_NODE_RPC_HOST, NAME_NODE_RPC_PORT)
                .usePlaintext()
                .build();

        this.nameNodeServiceGrpc = NameNodeServiceGrpc.newBlockingStub(channel);
    }

    @Override
    public int mkdir(String path) {
        MkdirRequest request = MkdirRequest.newBuilder().setPath(path).build();
        MkdirResponse response = nameNodeServiceGrpc.mkdir(request);

        logger.debug("mkdir response status is " + response.getStatus());

        return response.getStatus();
    }

    @Override
    public int createFile(String fileName) {
        CreateFileRequest request = CreateFileRequest.newBuilder().setFileName(fileName).build();
        CreateFileResponse response = nameNodeServiceGrpc.createFile(request);

        logger.debug("create file response status is " + response.getStatus());

        return response.getStatus();
    }

    @Override
    public int shutdown() {
        ShutdownRequest request = ShutdownRequest.newBuilder().setCode(1).build();
        ShutdownResponse response = nameNodeServiceGrpc.shutdown(request);

        logger.debug("shutdown response status is " + response.getStatus());

        return response.getStatus();
    }
}
