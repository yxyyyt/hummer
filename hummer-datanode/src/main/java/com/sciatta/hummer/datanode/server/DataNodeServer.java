package com.sciatta.hummer.datanode.server;

import com.sciatta.hummer.core.server.AbstractServer;
import com.sciatta.hummer.datanode.server.fs.DataNodeFileClient;
import com.sciatta.hummer.datanode.server.fs.DataNodeManager;
import com.sciatta.hummer.datanode.server.fs.DataNodeFileServer;
import com.sciatta.hummer.datanode.server.rpc.NameNodeRpcClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by Rain on 2023/1/2<br>
 * All Rights Reserved(C) 2017 - 2023 SCIATTA <br> <p/>
 * 数据节点服务
 */
public class DataNodeServer extends AbstractServer {
    private static final Logger logger = LoggerFactory.getLogger(DataNodeServer.class);

    private final NameNodeRpcClient nameNodeRpcClient;
    private final DataNodeFileClient dataNodeFileClient;
    private final DataNodeFileServer dataNodeFileServer;
    private final DataNodeManager dataNodeManager;

    public DataNodeServer() {
        super();

        this.nameNodeRpcClient = new NameNodeRpcClient();
        this.dataNodeFileClient = new DataNodeFileClient();
        this.dataNodeFileServer = new DataNodeFileServer(this, nameNodeRpcClient);
        this.dataNodeManager = new DataNodeManager(this.nameNodeRpcClient, this.dataNodeFileClient, this);
    }

    @Override
    protected void doStart() throws IOException {
        // 启动数据节点文件服务
        this.dataNodeFileServer.start();
    }

    @Override
    protected void doClose() {

    }
}
