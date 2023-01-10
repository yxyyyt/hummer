package com.sciatta.hummer.core.transport;

/**
 * Created by Rain on 2023/1/9<br>
 * All Rights Reserved(C) 2017 - 2023 SCIATTA <br> <p/>
 * 删除副本任务传输命令
 */
public class RemoveReplicaTaskCommand extends Command {
    /**
     * 文件名
     */
    private String fileName;

    public RemoveReplicaTaskCommand(int type) {
        super(type);
    }

    public RemoveReplicaTaskCommand(int type, String fileName) {
        super(type);
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
        return "RemoveReplicaTaskCommand{" +
                "type=" + type +
                ", fileName='" + fileName + '\'' +
                '}';
    }
}
