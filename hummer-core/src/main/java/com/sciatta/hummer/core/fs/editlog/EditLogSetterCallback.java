package com.sciatta.hummer.core.fs.editlog;

/**
 * Created by Rain on 2022/10/23<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 事务日志回调设置器
 */
public interface EditLogSetterCallback {
    /**
     * 个性化设置事务日志操作内容
     *
     * @param editLog 事务日志
     */
    void setEditLog(EditLog editLog);
}
