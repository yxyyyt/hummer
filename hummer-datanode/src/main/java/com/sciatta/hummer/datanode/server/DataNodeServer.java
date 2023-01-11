package com.sciatta.hummer.datanode.server;

import com.sciatta.hummer.client.fs.DataNodeFileClient;
import com.sciatta.hummer.client.rpc.NameNodeRpcClient;
import com.sciatta.hummer.core.config.ConfigManager;
import com.sciatta.hummer.core.server.AbstractServer;
import com.sciatta.hummer.datanode.config.DataNodeConfig;
import com.sciatta.hummer.datanode.fs.DataNodeFileServer;
import com.sciatta.hummer.datanode.fs.DataNodeManager;
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

    private final DataNodeFileServer dataNodeFileServer;

    public DataNodeServer() {
        super();

        logger.debug("data node server runs in {} mode", DataNodeConfig.getRunMode());

        NameNodeRpcClient nameNodeRpcClient = new NameNodeRpcClient();
        DataNodeFileClient dataNodeFileClient = new DataNodeFileClient();
        DataNodeManager dataNodeManager = new DataNodeManager(nameNodeRpcClient, dataNodeFileClient, this);

        this.dataNodeFileServer = new DataNodeFileServer(this, nameNodeRpcClient);
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
