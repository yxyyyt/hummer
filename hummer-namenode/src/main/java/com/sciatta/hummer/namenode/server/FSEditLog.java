package com.sciatta.hummer.namenode.server;

import com.sciatta.hummer.core.exception.HummerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static com.sciatta.hummer.namenode.server.NameNodeConfig.EDITS_LOG_BUFFER_LIMIT;

/**
 * Created by Rain on 2022/12/13<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 管理事务日志
 */
public class FSEditLog {
    private static final Logger logger = LoggerFactory.getLogger(FSEditLog.class);

    /**
     * 当前全局递增事务标识
     */
    private long globalTxId = 0L; // TODO 事务标识生成器服务，如果NameNode支持集群，事务标识需要全局唯一

    /**
     * 刷写磁盘最小事务标识
     */
    private long flushedMinTxId = 0L;

    /**
     * 刷写磁盘最大事务标识
     */
    private long flushedMaxTxId = 0L;

    /**
     * 双缓存
     */
    private final DoubleBuffer doubleBuffer = new DoubleBuffer();

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
    private final ThreadLocal<Long> localTxId = new ThreadLocal<>();

    public void logEdit(String content) {
        synchronized (this) {
            // 当前缓存已经写满，正在切换空闲缓存，当前线程等待切换完成
            while (isSchedulingBuffer) {
                try {
                    logger.debug("current cache has reached the schedule condition, waiting for scheduling buffer");
                    wait(1000);
                } catch (InterruptedException e) {
                    logger.error("{} exception while waiting for scheduling buffer", e.getMessage());
                    return;
                }
            }

            // 多线程同步顺序写入双缓存，保证txId顺序单调递增
            long txId = ++globalTxId;
            localTxId.set(txId);

            EditLog log = new EditLog(txId, content);

            try {
                doubleBuffer.write(log);   // 写入双缓存
            } catch (IOException e) {
                logger.error("{} exception need to shutdown and exit system while write editLog buffer", e.getMessage());
                throw new HummerException("%s exception need to shutdown and exit system while write editLog buffer", e.getMessage());
            }

            if (!doubleBuffer.shouldSyncToDisk()) {
                return; // 当前缓存有剩余空间，直接返回
            }

            isSchedulingBuffer = true;  // 设置调度buffer状态
        }

        logSync();  // 同步刷写磁盘
    }

    /**
     * 同步事务日志
     */
    private void logSync() {

        long myTxId = localTxId.get();

        // 交换缓冲区
        synchronized (this) {
            // 有线程正在同步磁盘缓存，当前线程写满当前缓存，等待磁盘同步完成才可以切换缓存
            // 如果频繁等待同步缓存，说明写入缓存速度远大于刷写磁盘速度，此时需要提高双缓存大小
            while (myTxId > flushedMaxTxId && isSyncRunning) {
                try {
                    logger.debug("waiting for sync finish, may be need to increase double buffer size [{}]!", EDITS_LOG_BUFFER_LIMIT);
                    wait(1000);
                } catch (InterruptedException e) {
                    logger.error("{} exception while waiting for sync", e.getMessage());
                }
            }

            // 当有非写入线程进入此处时，myTxId是0，如shutdown回调线程运行到此处时
            // 1、有一个线程正在同步，则直接返回即可，不需要同步
            // 2、有一个线程正在同步，有一个线程正在等待同步，不需要同步
            // 3、没有任何线程在同步，当前线程负责把缓存区剩余未同步的事务日志同步刷盘
            if (myTxId <= flushedMaxTxId && isSyncRunning) {
                logger.warn("myTxId[{}] <= flushedEndTxId[{}], exist sync thread, return now", myTxId, flushedMaxTxId);
                return;
            }

            doubleBuffer.setReadyToSync();

            // 更新刷写磁盘的最大txId
            flushedMaxTxId = myTxId;

            isSyncRunning = true;
            isSchedulingBuffer = false;
            notifyAll();
        }

        // 刷写磁盘
        try {
            doubleBuffer.flush();
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
     * 事务日志模型
     */
    static class EditLog {
        /**
         * 事务标识
         */
        long txId;

        /**
         * 事务日志内容
         */
        String content;

        public EditLog(long txId, String content) {
            this.txId = txId;
            this.content = content;
        }
    }

    /**
     * 内存双缓存，提高多线程写入缓存，及刷写磁盘效率，减少并发冲突
     */
    class DoubleBuffer {
        private final Logger logger = LoggerFactory.getLogger(DoubleBuffer.class);

        /**
         * 内存写入缓存
         */
        private EditLogBuffer currentBuffer = new EditLogBuffer();

        /**
         * 磁盘同步缓存
         */
        private EditLogBuffer syncBuffer = new EditLogBuffer();

        public void write(EditLog log) throws IOException {
            currentBuffer.write(log);
            logger.debug("{} write success, current buffer size is {}", log, currentBuffer.size());
        }

        /**
         * 是否满足同步刷写磁盘条件
         *
         * @return true，需要刷写磁盘；否则，不需要刷写磁盘
         */
        public boolean shouldSyncToDisk() {
            if (currentBuffer.size() >= EDITS_LOG_BUFFER_LIMIT) {
                logger.debug("current buffer[{}] >= EDIT_LOG_BUFFER_LIMIT[{}]", currentBuffer.size(), EDITS_LOG_BUFFER_LIMIT);
                return true;
            }
            return false;
        }

        /**
         * 交换缓存，为同步内存数据到磁盘做准备
         */
        public void setReadyToSync() {
            EditLogBuffer tmp = currentBuffer;
            currentBuffer = syncBuffer;
            syncBuffer = tmp;
            logger.debug("swap current buffer and sync buffer");
        }

        /**
         * 将缓存中的数据刷写磁盘
         */
        public void flush() throws IOException {
            syncBuffer.flush();
            syncBuffer.clear();
            logger.debug("flush sync buffer success");
        }
    }

    /**
     * 事务日志缓存
     */
    class EditLogBuffer {

        private final Logger logger = LoggerFactory.getLogger(EditLogBuffer.class);

        /**
         * 字节流输出缓存
         */
        private final ByteArrayOutputStream buffer;

        public EditLogBuffer() {
            buffer = new ByteArrayOutputStream(EDITS_LOG_BUFFER_LIMIT * 2);
        }

        /**
         * 写入内存缓存
         *
         * @param editLog 事务日志
         */
        public void write(EditLog editLog) throws IOException {
//                buffer.write(getJsonSerialization().toJson(editLog).getBytes());
//                buffer.write(StringUtil.getNewLine().getBytes());
        }

        /**
         * 获取内存缓冲区使用大小
         *
         * @return 内存缓冲区使用大小
         */
        public int size() {
            return buffer.size();
        }

        /**
         * 将内存缓冲区数据强制刷写到磁盘
         *
         * @throws IOException IO异常
         */
        public void flush() throws IOException {
            Path editsLogFile = NameNodeConfig.getEditsLogFile(++flushedMinTxId, flushedMaxTxId);
            logger.debug("sync disk path is " + editsLogFile.toFile().getPath());

            boolean flushFinish = false;

            try (FileChannel fileChannel = FileChannel.open(editsLogFile, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {

                fileChannel.write(ByteBuffer.wrap(buffer.toByteArray()));
                fileChannel.force(false);   // 强制刷写到磁盘
                flushFinish = true;
            } finally {
                if (flushFinish) {
                    flushedMinTxId = flushedMaxTxId;
                }
            }
        }

        /**
         * 将缓存清空
         */
        public void clear() {
            buffer.reset();
        }
    }
}
