package com.sciatta.hummer.datanode;

import com.sciatta.hummer.datanode.server.DataNodeServer;

import java.io.IOException;

/**
 * Created by Rain on 2022/12/13<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 数据节点
 */
public class DataNode {
    public static void main(String[] args) throws IOException {
        new DataNodeServer().start().keep();
    }
}
