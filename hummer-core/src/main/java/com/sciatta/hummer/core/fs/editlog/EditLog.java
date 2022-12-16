package com.sciatta.hummer.core.fs.editlog;

import com.sciatta.hummer.core.fs.editlog.operation.Operation;

/**
 * Created by Rain on 2022/12/14<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 事务日志模型
 */
public class EditLog {
    /**
     * 事务标识
     */
    private long txId;

    /**
     * 事务操作日志
     */
    private Operation operation;

    public EditLog(long txId) {
        this.txId = txId;
    }

    public long getTxId() {
        return txId;
    }

    public void setTxId(long txId) {
        this.txId = txId;
    }

    public Operation getOperation() {
        return operation;
    }

    public void setOperation(Operation operation) {
        this.operation = operation;
    }

    @Override
    public String toString() {
        return "EditLog{" +
                "txId=" + txId +
                ", operation=" + operation +
                '}';
    }
}
