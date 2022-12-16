package com.sciatta.hummer.client;

import com.sciatta.hummer.client.rpc.NameNodeRpcClient;

/**
 * Created by Rain on 2022/10/15<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 文件系统客户端实现
 */
public class FileSystemImpl implements FileSystem {
    private final FileSystem delegate;

    public FileSystemImpl() {
        this.delegate = new NameNodeRpcClient();
    }

    @Override
    public int mkdir(String path) {
        return this.delegate.mkdir(path);
    }

    @Override
    public int shutdown() {
        return this.delegate.shutdown();
    }
}
