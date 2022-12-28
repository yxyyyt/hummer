package com.sciatta.hummer.backupnode.fs;

import com.sciatta.hummer.core.fs.directory.FSImage;
import com.sciatta.hummer.core.server.Server;
import com.sciatta.hummer.core.util.PathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static com.sciatta.hummer.backupnode.config.BackupNodeConfig.CHECKPOINT_INTERVAL;
import static com.sciatta.hummer.backupnode.config.BackupNodeConfig.CHECKPOINT_PATH;
import static com.sciatta.hummer.backupnode.runtime.RuntimeParameter.LAST_CHECKPOINT_MAX_TX_ID;
import static com.sciatta.hummer.backupnode.runtime.RuntimeParameter.LAST_CHECKPOINT_TIMESTAMP;

/**
 * Created by Rain on 2022/10/23<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 定时生成内存目录树镜像
 */
public class FSImageCheckPointer extends Thread {

    private static final Logger logger = LoggerFactory.getLogger(FSImageCheckPointer.class);

    private final FSNameSystem fsNameSystem;
    private final Server server;

    public FSImageCheckPointer(FSNameSystem fsNameSystem, Server server) {
        this.fsNameSystem = fsNameSystem;
        this.server = server;
    }

    @Override
    public void run() {
        while (!server.isClosing()) {
            try {
                Thread.sleep(CHECKPOINT_INTERVAL);
            } catch (InterruptedException ignore) {
            }

            FSImage fsImage = fsNameSystem.getImage(
                    fsNameSystem.getRuntimeRepository().getLongParameter(LAST_CHECKPOINT_MAX_TX_ID, 0));
            if (fsImage != null) {
                logger.debug("start checkpoint, current fsImage max txId is " + fsImage.getMaxTxId());

                if (!beforeCheckPoint()) {
                    continue;
                }

                if (!doCheckPoint(fsImage)) {
                    continue;
                }

                afterCheckPoint(fsImage);
            } else {
                logger.debug("not have any new editsLog to replay, not need to checkpoint");
            }
        }
    }

    /**
     * 检查点任务前置处理
     *
     * @return true，可以继续
     */
    private boolean beforeCheckPoint() {
        try {
            Path checkPointFile = PathUtils.getFSImageFile(CHECKPOINT_PATH,
                    fsNameSystem.getRuntimeRepository().getLongParameter(LAST_CHECKPOINT_MAX_TX_ID, 0),
                    fsNameSystem.getRuntimeRepository().getLongParameter(LAST_CHECKPOINT_TIMESTAMP, 0),
                    false);

            // 不存在上一次备份的镜像
            if (!Files.exists(checkPointFile)) {
                logger.debug("last checkpoint file {} not exists", checkPointFile.toFile().getPath());
                return true;
            }

            Path lastCheckPointFile = PathUtils.getFSImageFile(CHECKPOINT_PATH,
                    fsNameSystem.getRuntimeRepository().getLongParameter(LAST_CHECKPOINT_MAX_TX_ID, 0),
                    fsNameSystem.getRuntimeRepository().getLongParameter(LAST_CHECKPOINT_TIMESTAMP, 0),
                    true);

            Files.move(checkPointFile, lastCheckPointFile); // 重命名上一次生成的镜像

            logger.debug("last checkpoint file {} rename to {}",
                    checkPointFile.toFile().getPath(),
                    lastCheckPointFile.toFile().getPath());

            return true;
        } catch (IOException e) {
            logger.error("{} while before checkpoint", e.getMessage());
        }

        return false;
    }

    /**
     * 检查点任务
     *
     * @param fsImage 镜像
     * @return true，可以继续
     */
    private boolean doCheckPoint(FSImage fsImage) {
        try {
            // 持久化本地镜像
            writeFSImage(fsImage);
            logger.debug("max txId is {}, timestamp is {} finish to write fsImage", fsImage.getMaxTxId(), fsImage.getTimestamp());

            // 上传远程镜像
            uploadFSImage(fsImage);
            logger.debug("max txId is {}, timestamp is {} finish to upload fsImage", fsImage.getMaxTxId(), fsImage.getTimestamp());

            return true;
        } catch (IOException e) {
            logger.error("{} while do checkpoint", e.getMessage());
        }
        return false;
    }

    /**
     * 持久化本地镜像
     *
     * @param fsImage 镜像
     */
    private void writeFSImage(FSImage fsImage) throws IOException {

        FileChannel fileChannel = null;

        try {
            Path checkPointFile = PathUtils.getFSImageFile(CHECKPOINT_PATH,
                    fsImage.getMaxTxId(), fsImage.getTimestamp(), false);

            fileChannel = FileChannel.open(checkPointFile, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            fileChannel.write(ByteBuffer.wrap(fsImage.getDirectory().getBytes()));
            fileChannel.force(false);   // 强制刷写到磁盘

        } catch (IOException e) {
            logger.error("{} while write fsImage", e.getMessage());
            throw e;
        } finally {
            if (fileChannel != null) {
                try {
                    fileChannel.close();
                } catch (IOException ignore) {
                }
            }
        }
    }

    /**
     * 向元数据节点同步上传镜像
     *
     * @param fsImage 镜像
     */
    private void uploadFSImage(FSImage fsImage) throws IOException {
        try {
            FSImageUploader fsImageUploader = new FSImageUploader(fsImage);
            fsImageUploader.upload();
        } catch (IOException e) {
            logger.error("{} while upload fsImage" + e.getMessage());
            throw e;
        }
    }

    /**
     * 检查点任务后置处理
     *
     * @param fsImage 镜像
     * @return true，可以继续
     */
    private boolean afterCheckPoint(FSImage fsImage) {
        try {
            // 若存在临时镜像文件，则删除
            Path lastCheckPointFile = PathUtils.getFSImageFile(CHECKPOINT_PATH,
                    fsNameSystem.getRuntimeRepository().getLongParameter(LAST_CHECKPOINT_MAX_TX_ID, 0),
                    fsNameSystem.getRuntimeRepository().getLongParameter(LAST_CHECKPOINT_TIMESTAMP, 0),
                    true);
            if (Files.deleteIfExists(lastCheckPointFile)) {
                logger.debug("delete last checkpoint file {}", lastCheckPointFile.toFile().getPath());
            }

            // 更新本地checkpoint信息
            fsNameSystem.getRuntimeRepository().setParameter(LAST_CHECKPOINT_MAX_TX_ID, fsImage.getMaxTxId());
            fsNameSystem.getRuntimeRepository().setParameter(LAST_CHECKPOINT_TIMESTAMP, fsImage.getTimestamp());

            return true;
        } catch (IOException e) {
            logger.error("{} while after checkpoint", e.getMessage());
        }

        return false;
    }
}
