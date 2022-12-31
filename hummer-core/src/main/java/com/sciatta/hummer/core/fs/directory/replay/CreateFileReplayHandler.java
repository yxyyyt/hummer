package com.sciatta.hummer.core.fs.directory.replay;

import com.sciatta.hummer.core.fs.directory.FSDirectory;
import com.sciatta.hummer.core.fs.editlog.EditLog;
import com.sciatta.hummer.core.fs.editlog.FSEditLog;
import com.sciatta.hummer.core.fs.editlog.operation.CreateFileOperation;
import com.sciatta.hummer.core.fs.editlog.operation.OperationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Rain on 2022/10/24<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 创建文件重放处理器
 */
public class CreateFileReplayHandler extends AbstractReplayHandler {
    private static final Logger logger = LoggerFactory.getLogger(CreateFileReplayHandler.class);

    public CreateFileReplayHandler(FSDirectory fsDirectory, FSEditLog fsEditLog) {
        super(fsDirectory, fsEditLog);
    }

    @Override
    public boolean accept(EditLog editLog) {
        return editLog.getOperation().getType().equals(OperationType.CREATE_FILE);
    }

    @Override
    protected boolean doReplay(EditLog editLog) {
        return this.fsDirectory.createFile(editLog.getTxId(),
                ((CreateFileOperation) editLog.getOperation()).getFileName());
    }
}
