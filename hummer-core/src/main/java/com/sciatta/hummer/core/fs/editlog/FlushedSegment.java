package com.sciatta.hummer.core.fs.editlog;

/**
 * Created by Rain on 2022/12/17<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 已同步到磁盘的事务日志分段
 */
public class FlushedSegment {
    private long minTxId;
    private long maxTxId;

    public FlushedSegment(long minTxId, long maxTxId) {
        this.minTxId = minTxId;
        this.maxTxId = maxTxId;
    }

    public long getMinTxId() {
        return minTxId;
    }

    public void setMinTxId(long minTxId) {
        this.minTxId = minTxId;
    }

    public long getMaxTxId() {
        return maxTxId;
    }

    public void setMaxTxId(long maxTxId) {
        this.maxTxId = maxTxId;
    }

    @Override
    public String toString() {
        return "FlushedSegment{" +
                "minTxId=" + minTxId +
                ", maxTxId=" + maxTxId +
                '}';
    }
}
