package com.sciatta.hummer.namenode.server;

import com.sciatta.hummer.core.fs.FSNameSystem;
import com.sciatta.hummer.core.server.AbstractServer;
import com.sciatta.hummer.namenode.config.NameNodeConfig;
import com.sciatta.hummer.namenode.datanode.DataNodeManager;
import com.sciatta.hummer.namenode.rpc.NameNodeRpcServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by Rain on 2022/12/13<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 元数据节点服务
 */
public class NameNodeServer extends AbstractServer {

    private static final Logger logger = LoggerFactory.getLogger(NameNodeServer.class);

    private final FSNameSystem fsNameSystem;
    private final DataNodeManager dataNodeManager;
    private final NameNodeRpcServer nameNodeRpcServer;

    public NameNodeServer() {
        this.fsNameSystem = new FSNameSystem(this,
                NameNodeConfig.EDITS_LOG_BUFFER_LIMIT,
                NameNodeConfig.EDITS_LOG_PATH);
        this.dataNodeManager = new DataNodeManager(this);
        this.nameNodeRpcServer = new NameNodeRpcServer(fsNameSystem, dataNodeManager,this);
    }

    @Override
    protected void doStart() throws IOException {
        // 启动RPC服务
        this.nameNodeRpcServer.start();

        // 启动数据节点管理服务
        this.dataNodeManager.start();
    }

    @Override
    protected void doClose() {
        // 停止RPC服务
        this.nameNodeRpcServer.stop();

        // 持久化元数据
        this.fsNameSystem.persist();
    }
}
