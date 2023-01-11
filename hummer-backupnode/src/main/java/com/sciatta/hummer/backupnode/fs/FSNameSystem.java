package com.sciatta.hummer.backupnode.fs;

import com.sciatta.hummer.backupnode.config.BackupNodeConfig;
import com.sciatta.hummer.core.fs.AbstractFSNameSystem;
import com.sciatta.hummer.core.fs.directory.FSImage;
import com.sciatta.hummer.core.server.Server;
import com.sciatta.hummer.core.util.PathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static com.sciatta.hummer.core.runtime.RuntimeParameter.LAST_CHECKPOINT_MAX_TX_ID;
import static com.sciatta.hummer.core.runtime.RuntimeParameter.LAST_CHECKPOINT_TIMESTAMP;

/**
 * Created by Rain on 2022/12/21<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 元数据管理
 */
public class FSNameSystem extends AbstractFSNameSystem {
    private final static Logger logger = LoggerFactory.getLogger(FSNameSystem.class);

    public FSNameSystem(Server server) {
        super(server);
    }

    @Override
    public int getEditsLogBufferLimit() {
        return BackupNodeConfig.getEditsLogBufferLimit();
    }

    @Override
    public String getEditsLogPath() {
        return PathUtils.getPathWithSlashAtLast(BackupNodeConfig.getEditsLogPath());
    }

    @Override
    public String getRuntimeRepositoryPath() {
        return PathUtils.getPathWithSlashAtLast(BackupNodeConfig.getRuntimeRepositoryPath());
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

    @Override
    protected FSImage loadFSImage() {
        FSImage fsImage = null;
        Path file = null;

        try {
            long maxTxId = runtimeRepository.getLongParameter(LAST_CHECKPOINT_MAX_TX_ID, 0);
            long timestamp = runtimeRepository.getLongParameter(LAST_CHECKPOINT_TIMESTAMP, 0);

            file = PathUtils.getFSImageFile(BackupNodeConfig.getCheckpointPath(), maxTxId, timestamp, false);
            byte[] allBytes = Files.readAllBytes(file);

            fsImage = new FSImage(maxTxId, new String(allBytes, 0, allBytes.length), timestamp);
            logger.debug("load fsImage from {}", file);
        } catch (IOException e) {
            logger.error("{} while load fsImage from {}", e.getMessage(), file);
        }

        return fsImage;
    }
}
