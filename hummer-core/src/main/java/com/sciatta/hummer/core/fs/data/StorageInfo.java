package com.sciatta.hummer.core.fs.data;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Rain on 2023/1/4<br>
 * All Rights Reserved(C) 2017 - 2023 SCIATTA <br> <p/>
 * 文件存储信息模型
 */
public class StorageInfo {
    /**
     * 存储上的所有文件名
     */
    private List<String> fileNames = new ArrayList<>();

    /**
     * 存储上的所有文件大小
     */
    private List<Long> fileSizes = new ArrayList<>();

    public List<String> getFileNames() {
        return fileNames;
    }

    public void setFileNames(List<String> fileNames) {
        this.fileNames = fileNames;
    }

    public List<Long> getFileSizes() {
        return fileSizes;
    }

    public void setFileSizes(List<Long> fileSizes) {
        this.fileSizes = fileSizes;
    }

    @Override
    public String toString() {
        return "StorageInfo{" +
                "fileNames=" + fileNames +
                ", fileSizes=" + fileSizes +
                '}';
    }
}
