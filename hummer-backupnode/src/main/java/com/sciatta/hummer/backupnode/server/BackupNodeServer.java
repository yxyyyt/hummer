package com.sciatta.hummer.backupnode.server;

import com.sciatta.hummer.backupnode.rpc.NameNodeRpcClient;
import com.sciatta.hummer.core.fs.FSNameSystem;
import com.sciatta.hummer.core.fs.editlog.operation.DummyOperation;
import com.sciatta.hummer.core.fs.editlog.operation.MkDirOperation;
import com.sciatta.hummer.core.fs.editlog.operation.Operation;
import com.sciatta.hummer.core.server.AbstractServer;
import com.sciatta.hummer.core.util.GsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.sciatta.hummer.backupnode.config.BackupNodeConfig.EDITS_LOG_BUFFER_LIMIT;
import static com.sciatta.hummer.backupnode.config.BackupNodeConfig.EDITS_LOG_PATH;

/**
 * Created by Rain on 2022/12/15<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 备份节点服务端
 */
public class BackupNodeServer extends AbstractServer {
    private static final Logger logger = LoggerFactory.getLogger(BackupNodeServer.class);

    private final NameNodeRpcClient nameNodeRpcClient;
    private final FSNameSystem fsNameSystem;
    private final FSEditsLogFetcher fsEditsLogFetcher;

    public BackupNodeServer() {
        this.nameNodeRpcClient = new NameNodeRpcClient();
        this.fsNameSystem = new FSNameSystem(this, EDITS_LOG_BUFFER_LIMIT, EDITS_LOG_PATH);
        this.fsEditsLogFetcher = new FSEditsLogFetcher(nameNodeRpcClient, fsNameSystem, this);

        // 注册运行时类型
        registerGsonRuntimeType();
    }

    @Override
    protected void doStart() throws IOException {
        // 启动同步事务日志管理组件
        this.fsEditsLogFetcher.start();
    }

    @Override
    protected void doClose() {
        // 持久化元数据
        this.fsNameSystem.persist();
    }

    /**
     * 注册运行时类型
     */
    @SuppressWarnings("unchecked")
    private void registerGsonRuntimeType() {
        GsonUtils.register(Operation.class, new Class[]{DummyOperation.class, MkDirOperation.class});
    }
}
