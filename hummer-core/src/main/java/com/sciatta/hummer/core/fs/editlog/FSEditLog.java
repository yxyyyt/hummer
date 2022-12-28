package com.sciatta.hummer.core.fs.editlog;

import com.google.gson.reflect.TypeToken;
import com.sciatta.hummer.core.exception.HummerException;
import com.sciatta.hummer.core.fs.AbstractFSNameSystem;
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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.sciatta.hummer.core.runtime.RuntimeParameter.*;

/**
 * Created by Rain on 2022/12/13<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 管理事务日志
 */
public class FSEditLog {
    private static final Logger logger = LoggerFactory.getLogger(FSEditLog.class);

    /**
     * 当前全局递增唯一事务标识
     */
    private volatile long txId = 0L; // TODO 事务标识生成器服务，如果NameNode支持集群，事务标识需要全局唯一

    /**
     * 同步磁盘唯一事务标识
     */
    private volatile long syncTxId;

    /**
     * 双缓存
     */
    private final DoubleBuffer doubleBuffer;

    /**
     * 当前是否正在刷写磁盘
     */
    private volatile boolean isSyncRunning = false;

    /**
     * 当前是否在交换缓冲区
     */
    private volatile boolean isSchedulingBuffer = false;

    /**
     * 线程本地事务标识
     */
    private final ThreadLocal<Long> localTxId = ThreadLocal.withInitial(() -> 0L);

    /**
     * 缓存已同步到磁盘的事务日志分段
     */
    private final List<FlushedSegment> flushedSegments = new CopyOnWriteArrayList<>();

    private final AbstractFSNameSystem fsNameSystem;
    private final Server server;
    protected final RuntimeRepository runtimeRepository;

    public FSEditLog(Server server, AbstractFSNameSystem fsNameSystem) {
        this.fsNameSystem = fsNameSystem;
        this.doubleBuffer = new DoubleBuffer(this.fsNameSystem);
        this.server = server;
        this.runtimeRepository = fsNameSystem.getRuntimeRepository();

        this.txId = this.runtimeRepository.getLongParameter(GLOBAL_TX_ID, 0);
        this.syncTxId = this.runtimeRepository.getLongParameter(SYNC_LOG_TX_ID, 0);

        List<FlushedSegment> flushedSegmentList = GsonUtils.fromJson(this.runtimeRepository.getJsonParameter(FLUSHED_SEGMENT_LIST, ""),
                new TypeToken<List<FlushedSegment>>() {
                }.getType());
        if (flushedSegmentList != null && flushedSegmentList.size() > 0) {
            this.flushedSegments.addAll(flushedSegmentList);
        }

        // 清除已备份为镜像之后冗余的事务日志
        FSEditLogCleaner fsEditLogCleaner = new FSEditLogCleaner();
        fsEditLogCleaner.start();
    }

    /**
     * 获取已同步到磁盘的事务日志分段
     *
     * @return 已同步到磁盘的事务日志分段
     */
    public List<FlushedSegment> getFlushedSegments() {
        return Collections.unmodifiableList(flushedSegments);
    }

    /**
     * 获取双缓存的内存缓冲区数据
     *
     * @return 内存缓冲区数据
     */
    public byte[] getBufferedDataFromDoubleBuffer() {
        synchronized (this) {
            return this.doubleBuffer.getBufferedDataFromCurrentBuffer();
        }
    }

    /**
     * 写入事务日志
     */
    public void logEdit(EditLog editLog) {
        synchronized (this) {
            // 当前缓存已经写满，正在切换空闲缓存，当前线程等待切换完成
            while (isSchedulingBuffer) {
                try {
                    logger.debug("current cache has reached the schedule condition, waiting for scheduling buffer");
                    wait(1000);
                } catch (InterruptedException e) {
                    logger.error("{} exception while waiting for scheduling buffer", e.getMessage());
                }
            }

            if (server.isClosing()) {
                logger.debug("server have been closed, then nothing to do");
                return;
            }

            if (editLog.getTxId() == Long.MIN_VALUE) {
                // 多线程同步顺序写入双缓存，保证txId顺序单调递增
                ++this.txId;
                editLog.setTxId(this.txId);
            } else {
                // 重放事务日志
                this.txId = editLog.getTxId();
            }

            localTxId.set(this.txId);

            try {
                doubleBuffer.write(editLog);   // 写入双缓存
            } catch (IOException e) {
                logger.error("{} exception while write editLog buffer", e.getMessage());
            }

            if (!doubleBuffer.shouldSyncToDisk()) {
                return; // 当前缓存有剩余空间，直接返回
            }

            isSchedulingBuffer = true;  // 设置调度buffer状态
        }

        logSync();  // 同步刷写磁盘
    }

    /**
     * 强制刷盘
     */
    public void forceSync() {
        logger.debug("force sync");
        logSync();

        this.runtimeRepository.setParameter(GLOBAL_TX_ID, this.txId);
        this.runtimeRepository.setParameter(SYNC_LOG_TX_ID, this.syncTxId);
        this.runtimeRepository.setParameter(FLUSHED_SEGMENT_LIST, GsonUtils.toJson(this.flushedSegments));
    }

    /**
     * 同步事务日志
     */
    private void logSync() {

        long myTxId = localTxId.get();  // 当前线程的事务标识

        // 交换缓冲区
        synchronized (this) {
            // 有线程正在同步磁盘缓存，当前线程写满当前缓存，等待磁盘同步完成才可以切换缓存
            // 如果频繁等待同步缓存，说明写入缓存速度远大于刷写磁盘速度，此时需要提高双缓存大小
            while (myTxId > syncTxId && isSyncRunning) {
                try {
                    logger.debug("waiting for sync finish, may be need to increase double buffer size {}!",
                            this.fsNameSystem.getEditsLogBufferLimit());
                    wait(1000);
                } catch (InterruptedException e) {
                    logger.error("{} exception while waiting for sync", e.getMessage());
                }
            }

            // TODO 逻辑检查
            // 当有非写入线程进入此处时，如果没有参与写入事务日志，则myTxId是0；若之前参与过写入事务日志，则myTxId小于syncTxId
            // 当shutdown回调线程运行到此处时：
            // 1、有一个线程正在同步，则直接返回即可，不需要同步
            // 2、有一个线程正在同步，有一个线程正在等待同步，不需要同步
            // 3、没有任何线程在同步，当前线程负责把缓存区剩余未同步的事务日志同步刷盘
            if (myTxId <= syncTxId && isSyncRunning) {
                logger.warn("myTxId[{}] <= syncTxId[{}], exist sync thread, return now", myTxId, syncTxId);
                return;
            } else if (myTxId <= syncTxId) {
                logger.warn("myTxId[{}] <= syncTxId[{}], not exist sync thread, need to sync remaining editLog",
                        myTxId, syncTxId);
            }

            doubleBuffer.setReadyToSync();

            // 更新刷写磁盘的最大事务标识
            syncTxId = myTxId;

            isSyncRunning = true;
            isSchedulingBuffer = false;
            notifyAll();
        }

        // 刷写磁盘
        try {
            doubleBuffer.flush(flushedSegments);
        } catch (IOException e) {
            logger.error("{} exception while flush editLog buffer", e.getMessage());
            throw new HummerException("%s exception while flush editLog buffer", e.getMessage());
        } finally {
            synchronized (this) {
                isSyncRunning = false;
                notifyAll();    // 唤醒正在等待当前线程完成同步磁盘的其他线程
            }
        }
    }

    /**
     * 清除已备份为镜像之后冗余的事务日志
     */
    private class FSEditLogCleaner extends Thread {

        @Override
        public void run() {
            while (!server.isClosing()) {
                doClean();
            }
        }

        /**
         * 执行清理
         */
        private void doClean() {
            List<FlushedSegment> removeFlushedSegment = new ArrayList<>();

            for (FlushedSegment flushedSegment : getFlushedSegments()) {
                if (runtimeRepository.getLongParameter(LAST_CHECKPOINT_MAX_TX_ID, 0)
                        >= flushedSegment.getMaxTxId()) {
                    Path fileDeleted = null;
                    try {
                        // 清除文件
                        fileDeleted = PathUtils.getEditsLogFile(fsNameSystem.getEditsLogPath(),
                                flushedSegment.getMinTxId(), flushedSegment.getMaxTxId());
                        if (Files.deleteIfExists(fileDeleted)) {
                            removeFlushedSegment.add(flushedSegment);
                            logger.debug("current checkpoint txId {}, clean editsLog file {} success",
                                    runtimeRepository.getLongParameter(LAST_CHECKPOINT_MAX_TX_ID, 0), fileDeleted);
                        }
                    } catch (IOException e) {
                        logger.error("{} while delete editsLog file {}", e.getMessage(), fileDeleted);
                    }
                } else {
                    // FlushedSegment集合有序，如果前边的不满足，后边的也不会满足
                    break;
                }
            }

            // 更新缓存
            if (removeFlushedSegment.size() > 0) {
                int beforeClean = flushedSegments.size();
                flushedSegments.removeAll(removeFlushedSegment);
                int afterClean = flushedSegments.size();
                logger.debug("before clean editsLog, flushed segment size is {}, and after clean, flushed segment size is {}",
                        beforeClean, afterClean);
            }
        }
    }
}
