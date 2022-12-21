package com.sciatta.hummer.backupnode.server;

import com.sciatta.hummer.backupnode.fs.FSEditsLogSynchronizer;
import com.sciatta.hummer.backupnode.fs.FSImageCheckPointer;
import com.sciatta.hummer.backupnode.rpc.NameNodeRpcClient;
import com.sciatta.hummer.backupnode.fs.FSNameSystem;
import com.sciatta.hummer.core.fs.directory.INode;
import com.sciatta.hummer.core.fs.directory.INodeDirectory;
import com.sciatta.hummer.core.fs.directory.INodeFile;
import com.sciatta.hummer.core.fs.editlog.operation.DummyOperation;
import com.sciatta.hummer.core.fs.editlog.operation.MkDirOperation;
import com.sciatta.hummer.core.fs.editlog.operation.Operation;
import com.sciatta.hummer.core.server.AbstractServer;
import com.sciatta.hummer.core.util.GsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.sciatta.hummer.backupnode.config.BackupNodeConfig.*;

/**
 * Created by Rain on 2022/12/15<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 备份节点服务端
 */
public class BackupNodeServer extends AbstractServer {
    private static final Logger logger = LoggerFactory.getLogger(BackupNodeServer.class);

    private final FSNameSystem fsNameSystem;
    private final FSEditsLogSynchronizer fsEditsLogSynchronizer;
    private final FSImageCheckPointer fsImageCheckPointer;

    public BackupNodeServer() {
        NameNodeRpcClient nameNodeRpcClient = new NameNodeRpcClient();

        this.fsNameSystem = new FSNameSystem(this);
        this.fsEditsLogSynchronizer = new FSEditsLogSynchronizer(nameNodeRpcClient, fsNameSystem, this);
        this.fsImageCheckPointer = new FSImageCheckPointer(fsNameSystem, this);

        // 注册运行时类型
        registerGsonRuntimeType();
    }

    @Override
    protected void doStart() throws IOException {
        // 恢复文件系统元数据
        this.fsNameSystem.restore();

        // 启动同步事务日志管理组件
        this.fsEditsLogSynchronizer.start();

        // 启动镜像检查点管理组件
        this.fsImageCheckPointer.start();
    }

    @Override
    protected void doClose() {
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
