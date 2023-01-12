package com.sciatta.hummer.namenode.fs.data;

import com.sciatta.hummer.core.fs.data.DataNodeInfo;

/**
 * Created by Rain on 2023/1/9<br>
 * All Rights Reserved(C) 2017 - 2023 SCIATTA <br> <p/>
 * 副本复制任务
 */
public class ReplicateTask {
    /**
     * 文件名
     */
    private String fileName;

    /**
     * 源复制数据节点
     */
    private DataNodeInfo source;

    /**
     * 目的复制数据节点
     */
    private DataNodeInfo destination;

    public ReplicateTask(String fileName, DataNodeInfo source, DataNodeInfo destination) {
        this.fileName = fileName;
        this.source = source;
        this.destination = destination;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public DataNodeInfo getSource() {
        return source;
    }

    public void setSource(DataNodeInfo source) {
        this.source = source;
    }

    public DataNodeInfo getDestination() {
        return destination;
    }

    public void setDestination(DataNodeInfo destination) {
        this.destination = destination;
    }

    @Override
    public String toString() {
        return "ReplicateTask{" +
                "fileName='" + fileName + '\'' +
                ", source=" + source +
                ", destination=" + destination +
                '}';
    }
}
