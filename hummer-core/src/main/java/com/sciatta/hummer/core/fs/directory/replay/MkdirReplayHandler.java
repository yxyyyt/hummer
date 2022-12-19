package com.sciatta.hummer.core.fs.directory.replay;

import com.sciatta.hummer.core.fs.directory.FSDirectory;
import com.sciatta.hummer.core.fs.editlog.EditLog;
import com.sciatta.hummer.core.fs.editlog.FSEditLog;
import com.sciatta.hummer.core.fs.editlog.operation.MkDirOperation;
import com.sciatta.hummer.core.fs.editlog.operation.OperationType;

/**
 * Created by Rain on 2022/10/24<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 创建目录重放处理器
 */
public class MkdirReplayHandler implements ReplayHandler {

    private final FSDirectory fsDirectory;
    private final FSEditLog fsEditLog;

    public MkdirReplayHandler(FSDirectory fsDirectory, FSEditLog fsEditLog) {
        this.fsDirectory = fsDirectory;
        this.fsEditLog = fsEditLog;
    }

    @Override
    public boolean accept(EditLog editLog) {
        return editLog.getOperation().getType().equals(OperationType.MKDIR);
    }

    @Override
    public void replay(EditLog editLog) {
        this.fsEditLog.logEdit(editLog);
        this.fsDirectory.mkdir(editLog.getTxId(), ((MkDirOperation) editLog.getOperation()).getPath());
    }
}
