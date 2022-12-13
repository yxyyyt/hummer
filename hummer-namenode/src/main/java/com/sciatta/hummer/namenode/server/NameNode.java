package com.sciatta.hummer.namenode.server;

/**
 * Created by Rain on 2022/12/13<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 元数据节点
 */
public class NameNode {
    /**
     * 管理元数据
     */
    private FSNameSystem fsNameSystem;

    /**
     * 管理集群中的所有数据节点
     */
    private DataNodeManager dataNodeManager;

    /**
     * 元数据节点RPC服务端，对外提供RPC服务
     */
    private NameNodeRpcServer nameNodeRpcServer;

    /**
     * 初始化
     */
    private void initialize() {
        this.fsNameSystem = new FSNameSystem();
        this.dataNodeManager = new DataNodeManager();
        this.nameNodeRpcServer = new NameNodeRpcServer(this.fsNameSystem, this.dataNodeManager);
    }

    /**
     * 运行
     */
    private void start() throws Exception {
        this.nameNodeRpcServer.start();
        this.nameNodeRpcServer.blockUntilShutdown();
    }

    public static void main(String[] args) throws Exception {
        NameNode namenode = new NameNode();
        namenode.initialize();
        namenode.start();
    }
}
