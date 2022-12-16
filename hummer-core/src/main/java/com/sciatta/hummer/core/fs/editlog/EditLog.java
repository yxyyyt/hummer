package com.sciatta.hummer.core.fs.editlog;

/**
 * Created by Rain on 2022/12/14<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 事务日志模型
 */
public class EditLog {
    /**
     * 事务标识
     */
    private final long txId;

    /**
     * 事务日志内容
     */
    private final String content;

    public EditLog(long txId, String content) {
        this.txId = txId;
        this.content = content;
    }

    public long getTxId() {
        return txId;
    }

    public String getContent() {
        return content;
    }

    @Override
    public String toString() {
        return "EditLog{" +
                "txId=" + txId +
                ", content='" + content + '\'' +
                '}';
    }
}
