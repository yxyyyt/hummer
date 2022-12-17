package com.sciatta.hummer.core.fs.directory.replay;

import com.sciatta.hummer.core.fs.FSNameSystem;
import com.sciatta.hummer.core.fs.editlog.EditLog;
import com.sciatta.hummer.core.fs.editlog.operation.MkDirOperation;
import com.sciatta.hummer.core.fs.editlog.operation.OperationType;

/**
 * Created by Rain on 2022/10/24<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 创建目录重放处理器
 */
public class MkdirReplayHandler implements ReplayHandler {

    private final FSNameSystem fsNameSystem;

    public MkdirReplayHandler(FSNameSystem fsNameSystem) {
        this.fsNameSystem = fsNameSystem;
    }

    @Override
    public boolean accept(EditLog editLog) {
        return editLog.getOperation().getType().equals(OperationType.MKDIR);
    }

    @Override
    public void replay(EditLog editLog) {
        fsNameSystem.mkdir(((MkDirOperation) editLog.getOperation()).getPath());
    }
}
