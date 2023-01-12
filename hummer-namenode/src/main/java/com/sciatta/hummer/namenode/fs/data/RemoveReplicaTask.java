package com.sciatta.hummer.namenode.fs.data;

/**
 * Created by Rain on 2023/1/11<br>
 * All Rights Reserved(C) 2017 - 2023 SCIATTA <br> <p/>
 * 副本删除任务
 */
public class RemoveReplicaTask {
    /**
     * 文件名
     */
    private String fileName;

    public RemoveReplicaTask(String fileName) {
        this.fileName = fileName;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    @Override
    public String toString() {
        return "RemoveReplicaTask{" +
                "fileName='" + fileName + '\'' +
                '}';
    }
}
