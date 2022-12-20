package com.sciatta.hummer.namenode.server;

import com.sciatta.hummer.core.fs.FSNameSystem;
import com.sciatta.hummer.core.fs.directory.INode;
import com.sciatta.hummer.core.fs.directory.INodeDirectory;
import com.sciatta.hummer.core.fs.directory.INodeFile;
import com.sciatta.hummer.core.fs.editlog.operation.DummyOperation;
import com.sciatta.hummer.core.fs.editlog.operation.MkDirOperation;
import com.sciatta.hummer.core.fs.editlog.operation.Operation;
import com.sciatta.hummer.core.server.AbstractServer;
import com.sciatta.hummer.core.util.GsonUtils;
import com.sciatta.hummer.namenode.datanode.DataNodeManager;
import com.sciatta.hummer.namenode.fs.FSImageUploadServer;
import com.sciatta.hummer.namenode.rpc.NameNodeRpcServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.sciatta.hummer.namenode.config.NameNodeConfig.*;

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
    private final FSImageUploadServer fsImageUploadServer;

    public NameNodeServer() {
        this.fsNameSystem = new FSNameSystem(this,
                EDITS_LOG_BUFFER_LIMIT,
                EDITS_LOG_PATH,
                RUNTIME_REPOSITORY_PATH
        );
        this.dataNodeManager = new DataNodeManager(this);
        this.nameNodeRpcServer = new NameNodeRpcServer(fsNameSystem, dataNodeManager, this);
        this.fsImageUploadServer = new FSImageUploadServer(fsNameSystem, this);

        // 注册运行时类型
        registerGsonRuntimeType();
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

    /**
     * 注册运行时类型
     */
    @SuppressWarnings("unchecked")
    private void registerGsonRuntimeType() {
        GsonUtils.register(Operation.class, new Class[]{DummyOperation.class, MkDirOperation.class});
        GsonUtils.register(INode.class, new Class[]{INodeFile.class, INodeDirectory.class});
    }
}
