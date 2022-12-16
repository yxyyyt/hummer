package com.sciatta.hummer.core.fs;

import com.sciatta.hummer.core.fs.directory.FSDirectory;
import com.sciatta.hummer.core.fs.editlog.FSEditLog;
import com.sciatta.hummer.core.fs.editlog.operation.MkDirOperation;
import com.sciatta.hummer.core.server.Server;

/**
 * Created by Rain on 2022/12/13<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 管理元数据
 */
public class FSNameSystem {
    private final FSDirectory fsDirectory;
    private final FSEditLog fsEditLog;

    public FSNameSystem(Server server, int editsLogBufferLimit, String editsLogPath) {
        this.fsDirectory = new FSDirectory();
        this.fsEditLog = new FSEditLog(server, editsLogBufferLimit, editsLogPath);
    }

    /**
     * 创建目录
     *
     * @param path 目录路径
     * @return 是否创建目录成功；true，创建成功；否则，创建失败
     */
    public boolean mkdir(String path) {
        this.fsDirectory.mkdir(path);
        this.fsEditLog.logEdit(editLog -> editLog.setOperation(new MkDirOperation(path)));
        return true;
    }

    /**
     * 持久化元数据
     */
    public void persist() {
        this.fsEditLog.forceSync();
    }
}
