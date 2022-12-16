package com.sciatta.hummer.namenode.datanode;

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
     * 主机名
     */
    private String hostname;

    /**
     * 上一次心跳的时间
     */
    private long latestHeartbeatTime = System.currentTimeMillis();

    public DataNodeInfo(String ip, String hostname) {
        this.ip = ip;
        this.hostname = hostname;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
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
}
