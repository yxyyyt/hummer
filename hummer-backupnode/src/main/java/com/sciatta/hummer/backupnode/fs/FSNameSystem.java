package com.sciatta.hummer.backupnode.fs;

import com.sciatta.hummer.core.fs.directory.FSImage;
import com.sciatta.hummer.core.server.Server;

import static com.sciatta.hummer.backupnode.config.BackupNodeConfig.*;

/**
 * Created by Rain on 2022/12/21<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 元数据管理
 */
public class FSNameSystem extends com.sciatta.hummer.core.fs.FSNameSystem {
    public FSNameSystem(Server server) {
        super(server);
    }

    @Override
    public int getEditsLogBufferLimit() {
        return EDITS_LOG_BUFFER_LIMIT;
    }

    @Override
    public String getEditsLogPath() {
        return EDITS_LOG_PATH;
    }

    @Override
    public String getRuntimeRepositoryPath() {
        return RUNTIME_REPOSITORY_PATH;
    }

    /**
     * 获取当前目录树对应的镜像
     *
     * @param lastCheckPointMaxTxId 上一次检查点的最大事务标识
     * @return 当前目录树对应的镜像
     */
    public FSImage getImage(long lastCheckPointMaxTxId) {
        return this.fsDirectory.getImage(lastCheckPointMaxTxId);
    }

    /**
     * 获得当前同步到的事务标识
     *
     * @return 当前同步到的事务标识
     */
    public long getSyncedTxId() {
        return this.fsDirectory.getMaxTxId();
    }
}
