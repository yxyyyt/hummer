package com.sciatta.hummer.core.fs;

import com.sciatta.hummer.core.exception.HummerException;
import com.sciatta.hummer.core.fs.directory.FSDirectory;
import com.sciatta.hummer.core.fs.directory.FSImage;
import com.sciatta.hummer.core.fs.directory.replay.MkdirReplayHandler;
import com.sciatta.hummer.core.fs.directory.replay.ReplayHandler;
import com.sciatta.hummer.core.fs.editlog.EditLog;
import com.sciatta.hummer.core.fs.editlog.FSEditLog;
import com.sciatta.hummer.core.fs.editlog.FlushedSegment;
import com.sciatta.hummer.core.fs.editlog.operation.MkDirOperation;
import com.sciatta.hummer.core.server.Server;
import com.sciatta.hummer.core.util.GsonUtils;
import com.sciatta.hummer.core.util.PathUtils;
import com.sciatta.hummer.core.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by Rain on 2022/12/13<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 管理元数据抽象实现
 */
public class FSNameSystem {
    private static final Logger logger = LoggerFactory.getLogger(FSNameSystem.class);

    private final FSDirectory fsDirectory;
    private final FSEditLog fsEditLog;

    /**
     * 上一次检查点的最大事务标识
     */
    private volatile long lastCheckPointMaxTxId;

    /**
     * 上一次检查点完成的时间戳
     */
    private volatile long lastCheckPointTimestamp;

    /**
     * 事务日志持久化路径
     */
    private final String editsLogPath;

    /**
     * 已注册的重放处理器
     */
    private final List<ReplayHandler> registeredReplayHandlers = new ArrayList<>();

    public FSNameSystem(Server server, int editsLogBufferLimit, String editsLogPath) {
        this.fsDirectory = new FSDirectory();
        this.fsEditLog = new FSEditLog(server, editsLogBufferLimit, editsLogPath);

        this.editsLogPath = editsLogPath;

        // 注册重放处理器
        registerReplayHandlers();
    }

    public long getLastCheckPointMaxTxId() {
        return lastCheckPointMaxTxId;
    }

    public void setLastCheckPointMaxTxId(long lastCheckPointMaxTxId) {
        this.lastCheckPointMaxTxId = lastCheckPointMaxTxId;
    }

    public long getLastCheckPointTimestamp() {
        return lastCheckPointTimestamp;
    }

    public void setLastCheckPointTimestamp(long lastCheckPointTimestamp) {
        this.lastCheckPointTimestamp = lastCheckPointTimestamp;
    }

    /**
     * 创建目录
     *
     * @param path 目录路径
     * @return 是否创建目录成功；true，创建成功；否则，创建失败
     */
    public boolean mkdir(String path) {
        EditLog editLog = new EditLog();
        editLog.setOperation(new MkDirOperation(path));

        this.fsEditLog.logEdit(editLog);
        this.fsDirectory.mkdir(editLog.getTxId(), path);

        return true;
    }

    /**
     * 持久化元数据
     */
    public void persist() {
        this.fsEditLog.forceSync();
    }

    /**
     * 获取已同步到磁盘的事务日志分段
     *
     * @return 已同步到磁盘的事务日志分段
     */
    public List<FlushedSegment> getFlushedSegments() {
        return this.fsEditLog.getFlushedSegments();
    }

    /**
     * 从双缓存获取事务日志
     *
     * @return 双缓存中的事务日志
     */
    @Deprecated
    public List<EditLog> getEditsLogFromDoubleBuffer() {
        List<EditLog> ans = new LinkedList<>();

        byte[] bufferData = this.fsEditLog.getBufferedDataFromDoubleBuffer();

        if (bufferData == null || bufferData.length <= 0) {
            return ans;
        }

        String[] jsons = new String(bufferData).split(StringUtils.getNewLine());
        Arrays.stream(jsons).forEach(json -> {
            ans.add(GsonUtils.fromJson(json, EditLog.class));
        });

        return ans;
    }

    /**
     * 从磁盘文件获取事务日志
     *
     * @param flushedSegment 已刷盘事务日志文件的事务日志分段
     * @return 磁盘文件中的事务日志
     */
    public List<EditLog> getEditsLogFromFlushedFile(FlushedSegment flushedSegment) {
        List<EditLog> ans = new LinkedList<>();

        try {
            List<String> jsons = Files.readAllLines(
                    PathUtils.getEditsLogFile(editsLogPath, flushedSegment.getMinTxId(), flushedSegment.getMaxTxId()), StandardCharsets.UTF_8);
            jsons.forEach(json -> {
                ans.add(GsonUtils.fromJson(json, EditLog.class));
            });
        } catch (IOException e) {
            logger.error("{} while get editsLog from flushed file", e.getMessage());
        }

        return ans;
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
     * 获取当前目录树对应的镜像
     *
     * @return 当前目录树对应的镜像
     */
    public FSImage getImage() {
        return this.fsDirectory.getImage(this.lastCheckPointMaxTxId);
    }

    /**
     * 注册重放处理器
     */
    private void registerReplayHandlers() {
        this.registeredReplayHandlers.add(new MkdirReplayHandler(this.fsDirectory, this.fsEditLog));
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
