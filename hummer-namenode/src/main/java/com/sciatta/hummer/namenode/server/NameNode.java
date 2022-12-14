package com.sciatta.hummer.namenode.server;

/**
 * Created by Rain on 2022/12/13<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 元数据节点
 */
public class NameNode {
    public static void main(String[] args) throws Exception {
        NameNodeRpcServer nameNodeRpcServer = new NameNodeRpcServer();
        nameNodeRpcServer.start();
        nameNodeRpcServer.keep();
    }
}
