package com.sciatta.hummer.backupnode.server;

import com.sciatta.hummer.backupnode.rpc.NameNodeRpcClient;
import com.sciatta.hummer.core.fs.FSNameSystem;
import com.sciatta.hummer.core.fs.editlog.EditLog;
import com.sciatta.hummer.core.server.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.sciatta.hummer.backupnode.config.BackupNodeConfig.BACKUP_NODE_FETCH_SIZE;

/**
 * Created by Rain on 2022/12/15<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 管理同步事务日志
 */
public class FSEditsLogFetcher extends Thread {
    private static final Logger logger = LoggerFactory.getLogger(FSEditsLogFetcher.class);

    private final FSNameSystem fsNameSystem;
    private final NameNodeRpcClient nameNodeRpcClient;
    private final Server server;

    /**
     * 当前从数据节点同步的最大事务标识
     */
    private long syncedTxId;

    public FSEditsLogFetcher(NameNodeRpcClient nameNodeRpcClient, FSNameSystem fsNameSystem, Server server) {
        this.fsNameSystem = fsNameSystem;
        this.nameNodeRpcClient = nameNodeRpcClient;
        this.server = server;
    }

    @Override
    public void run() {
        while (!server.isClosing()) {
            List<EditLog> editsLog = nameNodeRpcClient.fetchEditsLog(syncedTxId);

            // 没有数据，等待一会，重新拉取
            if (editsLog.size() == 0) {
                logger.debug("fetch editsLog is empty, wait a minute and repeat pull");
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e) {
                    logger.error("{} while fetch editsLog is empty", e.getMessage());
                }
                continue;
            }

            // 小于拉取一个批次的数据，等待一会再重放
            if (editsLog.size() < BACKUP_NODE_FETCH_SIZE) {
                logger.debug("fetch editsLog [{}] < BACKUP_NODE_FETCH_SIZE [{}], wait a minute",
                        editsLog.size(), BACKUP_NODE_FETCH_SIZE);
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e) {
                    logger.error("{} while fetch editsLog", e.getMessage());
                }
            }

            logger.debug("fetch editsLog size is " + editsLog.size());

            for (EditLog editLog : editsLog) {
                logger.debug("fetch editLog " + editLog);
                fsNameSystem.replay(editLog); // 重放事务日志
                syncedTxId = editLog.getTxId();
            }
        }
    }
}
