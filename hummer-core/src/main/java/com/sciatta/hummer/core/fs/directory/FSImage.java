package com.sciatta.hummer.core.fs.directory;

/**
 * Created by Rain on 2022/12/18<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 目录树镜像元数据模型
 */
public class FSImage {
    /**
     * 目录树对应的最大事务标识
     */
    private long maxTxId;

    /**
     * 生成目录树镜像的时间戳
     */
    private long timestamp;

    /**
     * 目录树JSON串
     */
    private String directory;

    public FSImage(long maxTxId, String directory) {
        this.maxTxId = maxTxId;
        this.directory = directory;
        this.timestamp = System.currentTimeMillis();
    }

    public long getMaxTxId() {
        return maxTxId;
    }

    public void setMaxTxId(long maxTxId) {
        this.maxTxId = maxTxId;
    }

    public String getDirectory() {
        return directory;
    }

    public void setDirectory(String directory) {
        this.directory = directory;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
