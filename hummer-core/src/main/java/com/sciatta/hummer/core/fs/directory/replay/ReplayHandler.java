package com.sciatta.hummer.core.fs.directory.replay;

import com.sciatta.hummer.core.fs.editlog.EditLog;

/**
 * Created by Rain on 2022/10/24<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 重放处理器
 */
public interface ReplayHandler {
    /**
     * 是否可以重放当前事务日志
     *
     * @param editLog 事务日志
     * @return true，当前处理器支持重放；否则，不支持
     */
    boolean accept(EditLog editLog);

    /**
     * 重放事务日志
     *
     * @param editLog 事务日志
     */
    void replay(EditLog editLog);
}
