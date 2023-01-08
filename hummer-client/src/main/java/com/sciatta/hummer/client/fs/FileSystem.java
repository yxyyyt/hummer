package com.sciatta.hummer.client.fs;

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
     * 上传文件
     *
     * @param file     文件字节数组
     * @param fileName 文件名
     * @param fileSize 文件大小
     * @return 上传文件是否成功；true，上传文件成功；否则，上传文件失败
     */
    boolean uploadFile(byte[] file, String fileName, long fileSize);

    /**
     * 下载文件
     *
     * @param filename 文件名
     * @return 文件字节数组
     */
    byte[] downloadFile(String filename);

    /**
     * 优雅停机
     *
     * @return 停机是否成功
     */
    int shutdown();
}
