package com.sciatta.hummer.core.fs;

import com.sciatta.hummer.core.exception.HummerException;
import com.sciatta.hummer.core.fs.directory.FSDirectory;
import com.sciatta.hummer.core.fs.directory.replay.MkdirReplayHandler;
import com.sciatta.hummer.core.fs.directory.replay.ReplayHandler;
import com.sciatta.hummer.core.fs.editlog.EditLog;
import com.sciatta.hummer.core.fs.editlog.FSEditLog;
import com.sciatta.hummer.core.runtime.RuntimeRepository;
import com.sciatta.hummer.core.server.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Rain on 2022/12/13<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 元数据管理抽象实现
 */
public abstract class FSNameSystem {
    private static final Logger logger = LoggerFactory.getLogger(FSNameSystem.class);

    protected final FSDirectory fsDirectory;
    protected final FSEditLog fsEditLog;
    protected final RuntimeRepository runtimeRepository;

    /**
     * 已注册的重放处理器
     */
    private final List<ReplayHandler> registeredReplayHandlers = new ArrayList<>();

    public FSNameSystem(Server server) {
        this.runtimeRepository = new RuntimeRepository(this);
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
        // 运行时仓库恢复数据
        try {
            this.runtimeRepository.restore();
        } catch (IOException e) {
            logger.error("{} while name system restore", e.getMessage());
            throw new HummerException(e);
        }

        // 恢复内存中的文件目录树
        restoreFSDirectory();
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
     * @param editLog 事务日志
     */
    public void replay(EditLog editLog) {
        for (ReplayHandler replay : registeredReplayHandlers) {
            if (replay.accept(editLog)) {
                replay.replay(editLog);
                return;
            }
        }
        logger.error("not any registered replay handlers");
        throw new HummerException("not any registered replay handlers");
    }

    /**
     * 注册重放处理器
     */
    private void registerReplayHandlers() {
        this.registeredReplayHandlers.add(new MkdirReplayHandler(this.fsDirectory, this.fsEditLog));
    }

    /**
     * 恢复内存中的文件目录树
     */
    private void restoreFSDirectory() {
        // 优先恢复镜像，然后重放事务日志
    }

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
