package com.sciatta.hummer.core.data;

/**
 * Created by Rain on 2022/12/13<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 数据节点模型
 */
public class DataNodeInfo {
    /**
     * IP地址
     */
    private String ip;

    /**
     * 文件上传服务端口
     */
    private int fileUploadServerPort;

    /**
     * 主机名
     */
    private String hostname;

    /**
     * 上一次心跳的时间
     */
    private long latestHeartbeatTime = System.currentTimeMillis();

    /**
     * 已存储数据的大小
     */
    private long storedDataSize;

    public DataNodeInfo(String ip, String hostname, int fileUploadServerPort) {
        this.ip = ip;
        this.hostname = hostname;
        this.fileUploadServerPort = fileUploadServerPort;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getFileUploadServerPort() {
        return fileUploadServerPort;
    }

    public void setFileUploadServerPort(int fileUploadServerPort) {
        this.fileUploadServerPort = fileUploadServerPort;
    }

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public long getLatestHeartbeatTime() {
        return latestHeartbeatTime;
    }

    public void setLatestHeartbeatTime(long latestHeartbeatTime) {
        this.latestHeartbeatTime = latestHeartbeatTime;
    }

    public long getStoredDataSize() {
        return storedDataSize;
    }

    public void setStoredDataSize(long storedDataSize) {
        this.storedDataSize = storedDataSize;
    }

    public void addStoredDataSize(long storedDataSize) {
        this.storedDataSize += storedDataSize;
    }

    public static String uniqueKey(String ip, String hostname) {
        return ip + "-" + hostname;
    }

    @Override
    public String toString() {
        return "DataNodeInfo{" +
                "ip='" + ip + '\'' +
                ", fileUploadServerPort=" + fileUploadServerPort +
                ", hostname='" + hostname + '\'' +
                ", latestHeartbeatTime=" + latestHeartbeatTime +
                ", storedDataSize=" + storedDataSize +
                '}';
    }
}
