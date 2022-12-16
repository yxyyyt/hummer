package com.sciatta.hummer.core.fs.editlog.operation;

/**
 * Created by Rain on 2022/10/23<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 事务日志操作
 */
public abstract class Operation {

    /**
     * 操作类型
     */
    protected OperationType type;

    public Operation(OperationType type) {
        this.type = type;
    }

    public OperationType getType() {
        return type;
    }

    public void setType(OperationType type) {
        this.type = type;
    }
}
