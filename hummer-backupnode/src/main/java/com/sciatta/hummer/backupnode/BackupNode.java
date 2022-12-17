package com.sciatta.hummer.backupnode;

import com.sciatta.hummer.backupnode.server.BackupNodeServer;

import java.io.IOException;

/**
 * Created by Rain on 2022/12/15<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 备份节点
 */
public class BackupNode {
    public static void main(String[] args) throws IOException {
        new BackupNodeServer().start().keep();
    }
}
