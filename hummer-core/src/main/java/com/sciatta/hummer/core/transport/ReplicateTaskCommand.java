package com.sciatta.hummer.core.transport;

/**
 * Created by Rain on 2023/1/9<br>
 * All Rights Reserved(C) 2017 - 2023 SCIATTA <br> <p/>
 * 复制副本任务传输命令
 */
public class ReplicateTaskCommand extends Command {
    /**
     * 文件名
     */
    private String fileName;

    /**
     * 主机名
     */
    private String hostname;

    /**
     * 端口
     */
    private int port;

    public ReplicateTaskCommand(int type) {
        super(type);
    }

    public ReplicateTaskCommand(int type, String fileName, String hostname, int port) {
        super(type);
        this.fileName = fileName;
        this.hostname = hostname;
        this.port = port;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    @Override
    public String toString() {
        return "ReplicateTaskCommand{" +
                "type=" + type +
                ", fileName='" + fileName + '\'' +
                ", hostname='" + hostname + '\'' +
                ", port=" + port +
                '}';
    }
}
