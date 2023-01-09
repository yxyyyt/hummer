package com.sciatta.hummer.namenode;

/**
 * Created by Rain on 2022/12/13<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 元数据节点
 */
public class NameNode {
    public static void main(String[] args) throws Exception {
        new NameNodeServer().start().keep();
    }
}
