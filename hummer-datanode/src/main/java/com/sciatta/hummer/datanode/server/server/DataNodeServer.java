package com.sciatta.hummer.datanode.server.server;

import com.sciatta.hummer.core.server.AbstractServer;
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
    private final DataNodeFileServer dataNodeFileServer;

    public DataNodeServer() {
        super();

        this.nameNodeRpcClient = new NameNodeRpcClient(this);
        this.dataNodeFileServer = new DataNodeFileServer(this);
    }

    @Override
    protected void doStart() throws IOException {
        // 发起注册
        try {
            this.nameNodeRpcClient.register();
        } catch (InterruptedException e) {
            logger.error("{} while send registration to name node", e.getMessage());
            throw new IOException(e);
        }

        // 发送心跳
        this.nameNodeRpcClient.heartbeat();

        // 启动数据节点文件服务
        this.dataNodeFileServer.start();
    }

    @Override
    protected void doClose() {

    }
}
