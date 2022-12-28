package com.sciatta.hummer.core.util;

import com.sciatta.hummer.core.fs.editlog.FlushedSegment;

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
    /**
     * 文件名分隔符
     */
    public static final String FILE_NAME_SPLIT = "-";

    /**
     * 文件后缀分隔符
     */
    public static final String FILE_SUFFIX = ".";

    /**
     * 获取路径，如果路径所在目录不存在，则递归创建
     *
     * @param path 路径字符串
     * @return 已存在目录的路径
     * @throws IOException IO异常
     */
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
        String file = getPathWithSlashAtLast(editsLogPath) +
                "edits" + FILE_NAME_SPLIT + flushedMinTxId + FILE_NAME_SPLIT + flushedMaxTxId +
                FILE_SUFFIX + "log";
        return PathUtils.getPathAndCreateDirectoryIfNotExists(file);
    }

    /**
     * 是否是有效的事务日志文件路径
     *
     * @param path 事务日志文件路径
     * @return true，有效；否则，无效
     */
    public static boolean isValidEditsLogFile(Path path) {
        return getFlushedSegmentFromEditsLogFile(path) != null;
    }

    /**
     * 从事务日志文件路径中获取事务日志分段
     *
     * @param editsLogFilePath 事务日志文件路径
     * @return 事务日志分段；如果路径不存在，不合法，则返回null
     */
    public static FlushedSegment getFlushedSegmentFromEditsLogFile(Path editsLogFilePath) {

        if (editsLogFilePath.getFileName() == null ||
                editsLogFilePath.getFileName().toString().trim().equals("") ||
                !editsLogFilePath.getFileName().toString().startsWith("edits" + FILE_NAME_SPLIT) ||
                !editsLogFilePath.getFileName().toString().endsWith(FILE_SUFFIX + "log")) {
            return null;
        }

        String[] split = editsLogFilePath.getFileName().toString()
                .replace("edits" + FILE_NAME_SPLIT, "")
                .replace(FILE_SUFFIX + "log", "")
                .split(FILE_NAME_SPLIT);

        if (split.length != 2) {
            return null;
        }

        FlushedSegment flushedSegment = null;
        try {
            int minTxId = Integer.parseInt(split[0]);
            int maxTxId = Integer.parseInt(split[1]);

            if (minTxId <= maxTxId) {
                flushedSegment = new FlushedSegment(minTxId, maxTxId);
            }
        } catch (Exception ignore) {
        }

        return flushedSegment;
    }

    /**
     * 获取镜像元数据文件存储路径
     *
     * @param checkPointPath      检查点持久化路径
     * @param maxTxId             镜像元数据最大事务标识
     * @param checkpointTimestamp 镜像元数据生成的时间戳
     * @param addLastSuffix       是否文件后增加 .last 后缀；true，增加后缀
     * @return 镜像元数据文件存储路径
     * @throws IOException IO异常
     */
    public static Path getFSImageFile(String checkPointPath, long maxTxId, long checkpointTimestamp, boolean addLastSuffix)
            throws IOException {
        StringBuilder sb = new StringBuilder(getPathWithSlashAtLast(checkPointPath));

        sb.append("fsimage").append(FILE_NAME_SPLIT).append(maxTxId).append(FILE_SUFFIX).append(checkpointTimestamp);

        if (addLastSuffix) {
            sb.append(FILE_SUFFIX).append("last");
        }

        return PathUtils.getPathAndCreateDirectoryIfNotExists(sb.toString());
    }

    /**
     * 获取运行时仓库文件存储路径
     *
     * @param runtimeRepositoryPath 运行时仓库持久化路径
     * @return 运行时仓库文件存储路径
     */
    public static Path getRuntimeRepositoryFile(String runtimeRepositoryPath) throws IOException {

        return PathUtils.getPathAndCreateDirectoryIfNotExists(getPathWithSlashAtLast(runtimeRepositoryPath) +
                "runtime" + FILE_SUFFIX + "repository");
    }

    /**
     * 提供目录的路径后面是否存在目录分隔符，如果不存在，则追加
     *
     * @param path 提供目录的路径
     * @return 路径后面带有目录分隔符的路径
     */
    public static String getPathWithSlashAtLast(String path) {
        if (path.lastIndexOf(File.separator) != (path.length() - 1)) {
            path += File.separator;
        }

        return path;
    }
}
