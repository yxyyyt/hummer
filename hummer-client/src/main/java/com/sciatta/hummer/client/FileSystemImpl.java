package com.sciatta.hummer.client;

import com.sciatta.hummer.rpc.MkdirRequest;
import com.sciatta.hummer.rpc.MkdirResponse;
import com.sciatta.hummer.rpc.NameNodeServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.sciatta.hummer.client.FileSystemClientConfig.NAME_NODE_RPC_HOST;
import static com.sciatta.hummer.client.FileSystemClientConfig.NAME_NODE_RPC_PORT;

/**
 * Created by Rain on 2022/10/15<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 文件系统客户端实现
 */
public class FileSystemImpl implements FileSystem {

    private static final Logger logger = LoggerFactory.getLogger(FileSystemImpl.class);

    private final NameNodeServiceGrpc.NameNodeServiceBlockingStub nameNodeServiceGrpc;

    public FileSystemImpl() {
        ManagedChannel channel = ManagedChannelBuilder.forAddress(NAME_NODE_RPC_HOST, NAME_NODE_RPC_PORT)
                .usePlaintext()
                .build();

        this.nameNodeServiceGrpc = NameNodeServiceGrpc.newBlockingStub(channel);
    }

    @Override
    public int mkdir(String path) {
        MkdirRequest request = MkdirRequest.newBuilder()
                .setPath(path)
                .build();

        MkdirResponse response = nameNodeServiceGrpc.mkdir(request);
        logger.debug("mkdir response status is " + response.getStatus());

        return response.getStatus();
    }
}
