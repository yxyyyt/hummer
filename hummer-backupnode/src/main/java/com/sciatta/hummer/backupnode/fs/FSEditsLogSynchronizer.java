package com.sciatta.hummer.backupnode.fs;

import com.sciatta.hummer.backupnode.config.BackupNodeConfig;
import com.sciatta.hummer.client.rpc.NameNodeRpcClient;
import com.sciatta.hummer.core.fs.editlog.EditLog;
import com.sciatta.hummer.core.server.Holder;
import com.sciatta.hummer.core.server.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by Rain on 2022/12/15<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 管理同步事务日志
 */
public class FSEditsLogSynchronizer extends Thread {
    private static final Logger logger = LoggerFactory.getLogger(FSEditsLogSynchronizer.class);

    private final FSNameSystem fsNameSystem;
    private final NameNodeRpcClient nameNodeRpcClient;
    private final Server server;

    public FSEditsLogSynchronizer(NameNodeRpcClient nameNodeRpcClient, FSNameSystem fsNameSystem, Server server) {
        this.fsNameSystem = fsNameSystem;
        this.nameNodeRpcClient = nameNodeRpcClient;
        this.server = server;
    }

    @Override
    public void run() {
        while (!server.isClosing()) {
            Holder<List<EditLog>> holder = new Holder<>();
            nameNodeRpcClient.fetchEditsLog(fsNameSystem.getSyncedTxId(), holder);

            // 没有数据，等待一会，重新拉取
            if (holder.get() == null || holder.get().size() == 0) {
                logger.debug("fetch editsLog is empty, wait a minute and repeat pull");
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e) {
                    logger.error("{} while fetch editsLog is empty", e.getMessage());
                }
                continue;
            }

            // 小于拉取一个批次的数据，等待一会再重放
            if (holder.get().size() < BackupNodeConfig.getBackupNodeFetchSize()) {
                logger.debug("fetch editsLog [{}] < BACKUP_NODE_FETCH_SIZE [{}], wait a minute",
                        holder.get().size(), BackupNodeConfig.getBackupNodeFetchSize());
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e) {
                    logger.error("{} while fetch editsLog", e.getMessage());
                }
            }

            for (EditLog editLog : holder.get()) {
                fsNameSystem.replay(editLog, true); // 重放事务日志
            }
        }
    }
}
