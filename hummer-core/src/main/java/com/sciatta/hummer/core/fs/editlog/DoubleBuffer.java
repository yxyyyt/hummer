package com.sciatta.hummer.core.fs.editlog;

import com.sciatta.hummer.core.util.GsonUtils;
import com.sciatta.hummer.core.util.PathUtils;
import com.sciatta.hummer.core.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

/**
 * Created by Rain on 2022/12/14<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 内存双缓存，提高多线程写入缓存，及刷写磁盘效率，减少并发冲突
 */
public class DoubleBuffer {
    private final Logger logger = LoggerFactory.getLogger(DoubleBuffer.class);

    /**
     * 内存写入缓存
     */
    private EditLogBuffer currentBuffer = new EditLogBuffer();

    /**
     * 磁盘同步缓存
     */
    private EditLogBuffer syncBuffer = new EditLogBuffer();

    /**
     * 上一次刷写磁盘事务标识
     */
    private long lastFlushedTxId = 0L;

    /**
     * 磁盘同步最大内存缓存大小，单位：字节
     */
    private final int editsLogBufferLimit;

    /**
     * 事务日志持久化路径
     */
    private final String editsLogPath;

    public DoubleBuffer(int editsLogBufferLimit, String editsLogPath) {
        this.editsLogBufferLimit = editsLogBufferLimit;
        this.editsLogPath = editsLogPath;
    }

    /**
     * 获取当前缓存的内存缓冲区数据
     *
     * @return 内存缓冲区数据
     */
    public byte[] getBufferedDataFromCurrentBuffer() {
        if (this.currentBuffer.size() == 0) {
            return null;
        }

        return this.currentBuffer.getBufferedData();
    }

    /**
     * 向双缓存写入事务日志
     *
     * @param editLog 事务日志
     * @throws IOException IO异常
     */
    public void write(EditLog editLog) throws IOException {
        currentBuffer.write(editLog);
        logger.debug("{} write success, current buffer size is {}", editLog, currentBuffer.size());
    }

    /**
     * 是否满足同步刷写磁盘条件
     *
     * @return true，需要刷写磁盘；否则，不需要刷写磁盘
     */
    public boolean shouldSyncToDisk() {
        if (currentBuffer.size() >= this.editsLogBufferLimit) {
            logger.debug("current buffer[{}] >= EDIT_LOG_BUFFER_LIMIT[{}]", currentBuffer.size(), this.editsLogBufferLimit);
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
     *
     * @param flushedSegments 已同步到磁盘的事务日志分段
     */
    public void flush(List<FlushedSegment> flushedSegments) throws IOException {
        logger.debug("start flush ->");
        long start = System.currentTimeMillis();

        syncBuffer.flush(flushedSegments);
        syncBuffer.clear();

        logger.debug("<- flush success, cost " + (System.currentTimeMillis() - start) + " ms");
    }

    /**
     * 事务日志缓存
     */
    class EditLogBuffer {

        private final Logger logger = LoggerFactory.getLogger(EditLogBuffer.class);

        /**
         * 当前缓存写入成功的事务标识
         */
        private long latestWriteTxId = 0L;

        /**
         * 字节流输出缓存
         */
        private final ByteArrayOutputStream buffer;

        public EditLogBuffer() {
            buffer = new ByteArrayOutputStream(editsLogBufferLimit * 2);
        }

        /**
         * 写入内存缓存
         *
         * @param editLog 事务日志
         */
        public void write(EditLog editLog) throws IOException {
            buffer.write(GsonUtils.toJson(editLog).getBytes());
            buffer.write(StringUtils.getNewLine().getBytes());
            latestWriteTxId = editLog.getTxId();
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
         * @param flushedSegments 已同步到磁盘的事务日志分段
         * @throws IOException IO异常
         */
        public void flush(List<FlushedSegment> flushedSegments) throws IOException {
            if (lastFlushedTxId == latestWriteTxId) {
                logger.debug("no editLog to flush");
                return;
            }

            Path editsLogFile = PathUtils.getEditsLogFile(editsLogPath, ++lastFlushedTxId, latestWriteTxId);
            logger.debug("sync disk path is " + editsLogFile.toFile().getPath());

            boolean flushFinish = false;

            try (FileChannel fileChannel = FileChannel.open(editsLogFile, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {

                fileChannel.write(ByteBuffer.wrap(buffer.toByteArray()));
                fileChannel.force(false);   // 强制刷写到磁盘
                flushFinish = true;
            } finally {
                if (flushFinish) {
                    flushedSegments.add(new FlushedSegment(lastFlushedTxId, latestWriteTxId));
                    lastFlushedTxId = latestWriteTxId;
                }
            }
        }

        /**
         * 将缓存清空
         */
        public void clear() {
            buffer.reset();
        }

        /**
         * 获取内存缓冲区数据
         *
         * @return 内存缓冲区数据
         */
        public byte[] getBufferedData() {
            return this.buffer.toByteArray();
        }
    }
}
