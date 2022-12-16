package com.sciatta.hummer.core.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by Rain on 2022/12/7<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 文件路径工具类
 */
public class PathUtils {
    public static Path getPathAndCreateDirectoryIfNotExists(String path) throws IOException {
        String directory = getDirectoryFromPath(path);
        if (!Files.exists(Paths.get(directory))) {
            Files.createDirectories(Paths.get(directory));
        }
        return Paths.get(path);
    }

    /**
     * 获取路径的目录
     *
     * @param path 路径
     * @return 目录
     */
    public static String getDirectoryFromPath(String path) {
        int end = path.lastIndexOf(File.separator);
        return path.substring(0, end + 1);
    }

    /**
     * 获取路径的文件
     *
     * @param path 路径
     * @return 文件
     */
    public static String getFileFromPath(String path) {
        int start = path.lastIndexOf(File.separator);
        return path.substring(start + 1);
    }

    /**
     * 获取事务日志文件存储路径
     *
     * @param editsLogPath   事务日志持久化路径
     * @param flushedMinTxId 刷写磁盘最小事务标识
     * @param flushedMaxTxId 刷写磁盘最大事务标识
     * @return 事务日志文件存储路径
     * @throws IOException IO异常
     */
    public static Path getEditsLogFile(String editsLogPath, long flushedMinTxId, long flushedMaxTxId) throws IOException {
        String file = editsLogPath + "edits-" + flushedMinTxId + "-" + flushedMaxTxId + ".log";
        return PathUtils.getPathAndCreateDirectoryIfNotExists(file);
    }
}
