package com.sciatta.hummer.core.fs.directory.replay.impl;

import com.sciatta.hummer.core.fs.directory.FSDirectory;
import com.sciatta.hummer.core.fs.directory.replay.AbstractReplayHandler;
import com.sciatta.hummer.core.fs.editlog.EditLog;
import com.sciatta.hummer.core.fs.editlog.FSEditLog;
import com.sciatta.hummer.core.fs.editlog.operation.impl.MkDirOperation;
import com.sciatta.hummer.core.fs.editlog.operation.OperationType;

/**
 * Created by Rain on 2022/10/24<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 创建目录重放处理器
 */
public class MkdirReplayHandler extends AbstractReplayHandler {

    public MkdirReplayHandler(FSDirectory fsDirectory, FSEditLog fsEditLog) {
        super(fsDirectory, fsEditLog);
    }

    @Override
    public boolean accept(EditLog editLog) {
        return editLog.getOperation().getType().equals(OperationType.MKDIR);
    }

    @Override
    protected boolean doReplay(EditLog editLog) {
        this.fsDirectory.mkdir(editLog.getTxId(), ((MkDirOperation) editLog.getOperation()).getPath());
        return true;
    }
}
