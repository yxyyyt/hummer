package com.sciatta.hummer.core.fs;

import com.sciatta.hummer.core.exception.HummerException;
import com.sciatta.hummer.core.fs.directory.FSDirectory;
import com.sciatta.hummer.core.fs.directory.FSImage;
import com.sciatta.hummer.core.fs.directory.INodeDirectory;
import com.sciatta.hummer.core.fs.directory.replay.CreateFileReplayHandler;
import com.sciatta.hummer.core.fs.directory.replay.MkdirReplayHandler;
import com.sciatta.hummer.core.fs.directory.replay.ReplayHandler;
import com.sciatta.hummer.core.fs.editlog.EditLog;
import com.sciatta.hummer.core.fs.editlog.FSEditLog;
import com.sciatta.hummer.core.runtime.RuntimeRepository;
import com.sciatta.hummer.core.server.Server;
import com.sciatta.hummer.core.util.GsonUtils;
import com.sciatta.hummer.core.util.PathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Created by Rain on 2022/12/13<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 元数据管理抽象实现
 */
public abstract class AbstractFSNameSystem {
    private static final Logger logger = LoggerFactory.getLogger(AbstractFSNameSystem.class);

    protected final FSDirectory fsDirectory;
    protected final FSEditLog fsEditLog;
    protected final RuntimeRepository runtimeRepository;

    /**
     * 已注册的重放处理器
     */
    private final List<ReplayHandler> registeredReplayHandlers = new ArrayList<>();

    public AbstractFSNameSystem(Server server) {
        // 运行时仓库恢复数据
        this.runtimeRepository = new RuntimeRepository(this);
        try {
            this.runtimeRepository.restore();
        } catch (IOException e) {
            logger.error("{} while restore runtime repository", e.getMessage());
            throw new HummerException(e);
        }

        this.fsDirectory = new FSDirectory();
        this.fsEditLog = new FSEditLog(server, this);

        // 注册重放处理器
        registerReplayHandlers();
    }

    public RuntimeRepository getRuntimeRepository() {
        return runtimeRepository;
    }

    /**
     * 恢复文件系统元数据
     */
    public void restore() {
        try {
            // 恢复内存中的文件目录树
            restoreFSDirectory();
        } catch (IOException e) {
            logger.error("{} while name system restore", e.getMessage());
            throw new HummerException(e);
        }
    }

    /**
     * 持久化文件系统元数据
     */
    public void save() {
        // 持久化内存中的事务日志
        this.fsEditLog.forceSync();

        // 运行时仓库保存数据
        try {
            this.runtimeRepository.save();
        } catch (IOException e) {
            logger.error("{} while name system save", e.getMessage());
            throw new HummerException(e);
        }
    }

    /**
     * 重放事务日志
     *
     * @param isLogEdit 是否记录事务日志；true，记录事务日志；否则，不需要记录事务日志。
     * @param editLog   事务日志
     */
    public void replay(EditLog editLog, boolean isLogEdit) {
        for (ReplayHandler replay : registeredReplayHandlers) {
            if (replay.accept(editLog)) {
                replay.replay(editLog, isLogEdit);
                return;
            }
        }
        logger.error("not any registered replay handler for editLog " + editLog);
        throw new HummerException("not any registered replay handler for editLog " + editLog);
    }

    /**
     * 注册重放处理器
     */
    private void registerReplayHandlers() {
        this.registeredReplayHandlers.add(new MkdirReplayHandler(this.fsDirectory, this.fsEditLog));
        this.registeredReplayHandlers.add(new CreateFileReplayHandler(this.fsDirectory, this.fsEditLog));
    }

    /**
     * 恢复内存中的文件目录树
     */
    private void restoreFSDirectory() throws IOException {
        // 加载镜像
        FSImage fsImage = loadFSImage();
        if (fsImage != null) {
            this.fsDirectory.setDirTree(GsonUtils.fromJson(fsImage.getDirectory(), INodeDirectory.class));
            this.fsDirectory.setMaxTxId(fsImage.getMaxTxId());
            logger.debug("load fsImage success, maxTxId is {}, timestamp is {}",
                    fsImage.getMaxTxId(), fsImage.getTimestamp());
        } else {
            logger.debug("no fsImage to load");
        }

        // 加载事务日志并重放
        if (fsImage != null) {
            loadEditsLogAndReplay(fsImage.getMaxTxId());
        } else {
            loadEditsLogAndReplay(0);
        }
    }

    /**
     * 加载事务日志并重放
     *
     * @param fsImageMaxTxId 镜像的最大事务标识
     * @throws IOException IO异常
     */
    private void loadEditsLogAndReplay(long fsImageMaxTxId) throws IOException {
        List<Path> editsLogFiles = Files.list(PathUtils.getPathAndCreateDirectoryIfNotExists(getEditsLogPath()))
                .filter(PathUtils::isValidEditsLogFile)
                .filter(
                        path -> fsImageMaxTxId < Objects.requireNonNull(PathUtils.getFlushedSegmentFromEditsLogFile(path)).getMaxTxId()
                )
                .sorted((p1, p2) -> (int) (Objects.requireNonNull(PathUtils.getFlushedSegmentFromEditsLogFile(p1)).getMaxTxId() -
                        Objects.requireNonNull(PathUtils.getFlushedSegmentFromEditsLogFile(p2)).getMaxTxId()))
                .collect(Collectors.toList());

        if (editsLogFiles.size() == 0) {
            logger.debug("no editsLog to load");
            return;
        }

        long sequenceTxId = fsImageMaxTxId;
        for (Path editLogFile : editsLogFiles) {
            List<String> editsLog = Files.readAllLines(editLogFile);
            int replayCount = 0;
            for (String log : editsLog) {
                EditLog editLog = GsonUtils.fromJson(log, EditLog.class);
                if (editLog.getTxId() >= sequenceTxId + 1) {    // 按序恢复，txId可能会中断
                    this.replay(editLog, false);
                    replayCount++;
                    sequenceTxId = editLog.getTxId();
                }
            }
            logger.debug("load from {}, include {} editsLog to replay", editLogFile, replayCount);
        }
    }

    /**
     * 加载镜像
     *
     * @return 镜像；若不存，则返回null
     */
    protected abstract FSImage loadFSImage();

    /**
     * 获取磁盘同步最大内存缓存大小，单位：字节
     *
     * @return 磁盘同步最大内存缓存大小，单位：字节
     */
    public abstract int getEditsLogBufferLimit();

    /**
     * 获取事务日志持久化路径
     *
     * @return 事务日志持久化路径
     */
    public abstract String getEditsLogPath();

    /**
     * 获取运行时仓库持久化路径
     *
     * @return 运行时仓库持久化路径
     */
    public abstract String getRuntimeRepositoryPath();
}
