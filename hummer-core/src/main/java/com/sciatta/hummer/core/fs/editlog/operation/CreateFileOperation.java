package com.sciatta.hummer.core.fs.editlog.operation;

/**
 * Created by Rain on 2022/10/23<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 创建文件操作
 */
public class CreateFileOperation extends Operation {
    /**
     * 文件路径
     */
    private String fileName;

    public CreateFileOperation(String fileName) {
        super(OperationType.CREATE_FILE);
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
        return "CreateFileOperation{" +
                "fileName='" + fileName + '\'' +
                ", type=" + type +
                '}';
    }
}
