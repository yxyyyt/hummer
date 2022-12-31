package com.sciatta.hummer.namenode.fs;

import com.sciatta.hummer.core.fs.AbstractFSNameSystem;
import com.sciatta.hummer.core.fs.directory.FSImage;
import com.sciatta.hummer.core.fs.editlog.EditLog;
import com.sciatta.hummer.core.fs.editlog.FlushedSegment;
import com.sciatta.hummer.core.fs.editlog.operation.CreateFileOperation;
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
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static com.sciatta.hummer.core.runtime.RuntimeParameter.LAST_CHECKPOINT_MAX_TX_ID;
import static com.sciatta.hummer.core.runtime.RuntimeParameter.LAST_CHECKPOINT_TIMESTAMP;
import static com.sciatta.hummer.namenode.config.NameNodeConfig.*;

/**
 * Created by Rain on 2022/12/21<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 元数据管理
 */
public class FSNameSystem extends AbstractFSNameSystem {
    private static final Logger logger = LoggerFactory.getLogger(FSNameSystem.class);

    public FSNameSystem(Server server) {
        super(server);
    }

    @Override
    public int getEditsLogBufferLimit() {
        return EDITS_LOG_BUFFER_LIMIT;
    }

    @Override
    public String getEditsLogPath() {
        return EDITS_LOG_PATH;
    }

    @Override
    public String getRuntimeRepositoryPath() {
        return RUNTIME_REPOSITORY_PATH;
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
     * 创建文件
     *
     * @param fileName 文件名
     * @return 是否创建文件成功；true，创建成功；否则，创建失败
     */
    public boolean createFile(String fileName) {
        EditLog editLog = new EditLog();
        editLog.setOperation(new CreateFileOperation(fileName));

        this.fsEditLog.logEdit(editLog);
        return this.fsDirectory.createFile(editLog.getTxId(), fileName);
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
            List<String> jsons = Files.readAllLines(PathUtils.getEditsLogFile(
                    getEditsLogPath(), flushedSegment.getMinTxId(), flushedSegment.getMaxTxId()), StandardCharsets.UTF_8);
            jsons.forEach(json -> {
                ans.add(GsonUtils.fromJson(json, EditLog.class));
            });
        } catch (IOException e) {
            logger.error("{} while get editsLog from flushed file", e.getMessage());
        }

        return ans;
    }

    @Override
    protected FSImage loadFSImage() {
        FSImage fsImage = null;
        Path file = null;

        try {
            long maxTxId = runtimeRepository.getLongParameter(LAST_CHECKPOINT_MAX_TX_ID, 0);
            long timestamp = runtimeRepository.getLongParameter(LAST_CHECKPOINT_TIMESTAMP, 0);

            file = PathUtils.getFSImageFile(CHECKPOINT_PATH, maxTxId, timestamp, false);
            byte[] allBytes = Files.readAllBytes(file);

            fsImage = new FSImage(maxTxId, new String(allBytes, 0, allBytes.length), timestamp);
            logger.debug("load fsImage from {}", file);
        } catch (IOException e) {
            logger.error("{} while load fsImage from {}", e.getMessage(), file);
        }

        return fsImage;
    }
}
