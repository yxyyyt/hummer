package com.sciatta.hummer.core.data;

/**
 * Created by Rain on 2022/12/13<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 数据节点模型
 */
public class DataNodeInfo {
    /**
     * 主机名
     */
    private String hostname;

    /**
     * 端口
     */
    private int port;

    /**
     * 最近一次心跳的时间
     */
    private long latestHeartbeatTime;

    /**
     * 最近一次注册的时间
     */
    private long latestRegisterTime;

    /**
     * 已存储数据的大小
     */
    private long storedDataSize;

    public DataNodeInfo(String hostname, int port) {
        this.hostname = hostname;
        this.port = port;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
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

    public long getLatestRegisterTime() {
        return latestRegisterTime;
    }

    public void setLatestRegisterTime(long latestRegisterTime) {
        this.latestRegisterTime = latestRegisterTime;
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

    /**
     * 获取数据节点唯一标识
     *
     * @param hostname 主机名
     * @param port     端口
     * @return 数据节点唯一标识
     */
    public static String uniqueKey(String hostname, int port) {
        return hostname + ":" + port;
    }

    @Override
    public String toString() {
        return "DataNodeInfo{" +
                "hostname='" + hostname + '\'' +
                ", port=" + port +
                ", latestHeartbeatTime=" + latestHeartbeatTime +
                ", latestRegisterTime=" + latestRegisterTime +
                ", storedDataSize=" + storedDataSize +
                '}';
    }
}
