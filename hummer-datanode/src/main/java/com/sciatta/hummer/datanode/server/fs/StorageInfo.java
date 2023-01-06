package com.sciatta.hummer.datanode.server.fs;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Rain on 2023/1/4<br>
 * All Rights Reserved(C) 2017 - 2023 SCIATTA <br> <p/>
 * 文件存储信息模型
 */
public class StorageInfo {
    /**
     * 存储上的所有文件
     */
    private List<String> fileNames = new ArrayList<>();

    /**
     * 存储数据大小
     */
    private long storedDataSize = 0;

    public List<String> getFileNames() {
        return fileNames;
    }

    public void setFileNames(List<String> fileNames) {
        this.fileNames = fileNames;
    }

    public long getStoredDataSize() {
        return storedDataSize;
    }

    public void setStoredDataSize(long storedDataSize) {
        this.storedDataSize = storedDataSize;
    }

    public void addFileName(String fileName) {
        this.fileNames.add(fileName);
    }

    public void addStoredDataSize(long storedDataSize) {
        this.storedDataSize += storedDataSize;
    }

    @Override
    public String toString() {
        return "StorageInfo{" +
                "fileNames=" + fileNames +
                ", storedDataSize=" + storedDataSize +
                '}';
    }
}
