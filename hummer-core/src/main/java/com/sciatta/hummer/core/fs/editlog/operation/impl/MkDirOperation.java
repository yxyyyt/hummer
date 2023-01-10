package com.sciatta.hummer.core.fs.editlog.operation.impl;

import com.sciatta.hummer.core.fs.editlog.operation.Operation;
import com.sciatta.hummer.core.fs.editlog.operation.OperationType;

/**
 * Created by Rain on 2022/10/23<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 创建目录操作
 */
public class MkDirOperation extends Operation {
    /**
     * 目录路径
     */
    private String path;

    public MkDirOperation(String path) {
        super(OperationType.MKDIR);
        this.path = path;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    @Override
    public String toString() {
        return "MkDirOperation{" +
                "path='" + path + '\'' +
                ", type=" + type +
                '}';
    }
}
