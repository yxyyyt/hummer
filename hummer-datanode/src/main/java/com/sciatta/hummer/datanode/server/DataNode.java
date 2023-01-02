package com.sciatta.hummer.datanode.server;

import com.sciatta.hummer.datanode.server.fs.DataNodeFileServer;

/**
 * Created by Rain on 2022/12/13<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 数据节点
 */
public class DataNode {
    /**
     * 是否正在运行
     */
    private volatile Boolean shouldRun; // 统一运行方式

    /**
     * 初始化
     */
    private void initialize() {
        this.shouldRun = true;
        NameNodeOfferService offerService = new NameNodeOfferService();
        offerService.start();

        DataNodeFileServer dataNodeFileServer = new DataNodeFileServer();
        dataNodeFileServer.start();
    }

    /**
     * 运行
     */
    private void run() {
        try {
            while (shouldRun) {
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        DataNode datanode = new DataNode();
        datanode.initialize();
        datanode.run();
    }
}
