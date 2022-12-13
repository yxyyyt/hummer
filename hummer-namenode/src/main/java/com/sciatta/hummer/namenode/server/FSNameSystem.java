package com.sciatta.hummer.namenode.server;

/**
 * Created by Rain on 2022/12/13<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 管理元数据
 */
public class FSNameSystem {
    /**
     * 管理内存中的文件目录树
     */
    private FSDirectory fsDirectory;

    /**
     * 管理事务日志
     */
    private FSEditLog fsEditLog;

    public FSNameSystem() {
        this.fsDirectory = new FSDirectory();
        this.fsEditLog = new FSEditLog();
    }

    /**
     * 创建目录
     *
     * @param path 目录路径
     * @return 是否创建目录成功；true，创建成功；否则，创建失败
     */
    public boolean mkdir(String path) {
        this.fsDirectory.mkdir(path);
        this.fsEditLog.logEdit("创建了一个目录：" + path);
        return true;
    }
}
