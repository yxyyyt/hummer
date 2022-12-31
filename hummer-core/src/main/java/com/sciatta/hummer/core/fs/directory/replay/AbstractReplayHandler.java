package com.sciatta.hummer.core.fs.directory.replay;

import com.sciatta.hummer.core.fs.directory.FSDirectory;
import com.sciatta.hummer.core.fs.editlog.EditLog;
import com.sciatta.hummer.core.fs.editlog.FSEditLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Rain on 2022/12/30<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 重放处理器抽象实现
 */
public abstract class AbstractReplayHandler implements ReplayHandler {
    private static final Logger logger = LoggerFactory.getLogger(AbstractReplayHandler.class);

    protected final FSDirectory fsDirectory;
    protected final FSEditLog fsEditLog;

    public AbstractReplayHandler(FSDirectory fsDirectory, FSEditLog fsEditLog) {
        this.fsDirectory = fsDirectory;
        this.fsEditLog = fsEditLog;
    }

    @Override
    public void replay(EditLog editLog, boolean isLogEdit) {
        if (isLogEdit) {
            this.fsEditLog.logEdit(editLog);
        }

        boolean ans = doReplay(editLog);

        if (ans) {
            logger.debug("==> replay editLog success {}", editLog);
        } else {
            logger.warn("==x replay editLog fail {}", editLog);
        }
    }

    protected abstract boolean doReplay(EditLog editLog);
}
