package com.sciatta.hummer.client;

/**
 * Created by Rain on 2022/10/15<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 文件系统客户端接口
 */
public interface FileSystem {
    /**
     * 创建目录
     *
     * @param path 目录路径
     * @return 创建目录响应
     */
    int mkdir(String path);

    /**
     * 创建文件
     *
     * @param fileName 文件名
     * @return 创建文件响应
     */
    int createFile(String fileName);

    /**
     * 优雅停机
     *
     * @return 停机是否成功
     */
    int shutdown();
}
