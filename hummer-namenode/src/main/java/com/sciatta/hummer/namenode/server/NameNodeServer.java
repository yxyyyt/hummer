package com.sciatta.hummer.namenode.server;

import com.sciatta.hummer.core.server.AbstractServer;
import com.sciatta.hummer.namenode.fs.data.DataNodeManager;
import com.sciatta.hummer.namenode.fs.backup.FSImageUploadServer;
import com.sciatta.hummer.namenode.fs.FSNameSystem;
import com.sciatta.hummer.namenode.rpc.NameNodeRpcServer;

import java.io.IOException;

/**
 * Created by Rain on 2022/12/13<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 元数据节点服务
 */
public class NameNodeServer extends AbstractServer {
    private final FSNameSystem fsNameSystem;
    private final DataNodeManager dataNodeManager;
    private final NameNodeRpcServer nameNodeRpcServer;
    private final FSImageUploadServer fsImageUploadServer;

    public NameNodeServer() {
        super();

        this.fsNameSystem = new FSNameSystem(this);
        this.dataNodeManager = new DataNodeManager(this);
        this.nameNodeRpcServer = new NameNodeRpcServer(fsNameSystem, dataNodeManager, this);
        this.fsImageUploadServer = new FSImageUploadServer(fsNameSystem, this);
    }

    @Override
    protected void doStart() throws IOException {
        // 恢复文件系统元数据
        this.fsNameSystem.restore();

        // 启动RPC服务
        this.nameNodeRpcServer.start();

        // 启动数据节点管理服务
        this.dataNodeManager.start();

        // 启动镜像上传接收服务
        this.fsImageUploadServer.start();
    }

    @Override
    protected void doClose() {
        // 停止RPC服务
        this.nameNodeRpcServer.stop();

        // 持久化文件系统元数据
        this.fsNameSystem.save();
    }
}
