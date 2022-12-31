package com.sciatta.hummer.core.fs.directory;

import com.sciatta.hummer.core.util.GsonUtils;
import com.sciatta.hummer.core.util.PathUtils;
import com.sciatta.hummer.core.util.StringUtils;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by Rain on 2022/12/13<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 管理内存中的文件目录树
 */
public class FSDirectory {

    private INodeDirectory dirTree;

    /**
     * 当前目录树对应的最大事务标识
     */
    private volatile long maxTxId;

    /**
     * 读写锁
     */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public FSDirectory() {
        this.dirTree = new INodeDirectory("/");
    }

    public long getMaxTxId() {
        return maxTxId;
    }

    public void setMaxTxId(long maxTxId) {
        this.maxTxId = maxTxId;
    }

    public INodeDirectory getDirTree() {
        return dirTree;
    }

    public void setDirTree(INodeDirectory dirTree) {
        this.dirTree = dirTree;
    }

    /**
     * 读锁加锁
     */
    private void readLock() {
        lock.readLock().lock();
    }

    /**
     * 读锁解锁
     */
    private void readUnLock() {
        lock.readLock().unlock();
    }

    /**
     * 写锁加锁
     */
    private void writeLock() {
        lock.writeLock().lock();
    }

    /**
     * 写锁解锁
     */
    private void writeUnLock() {
        lock.writeLock().unlock();
    }

    /**
     * 获取当前目录树对应的镜像
     *
     * @param lastCheckPointMaxTxId 上一次检查点的最大事务标识
     * @return 当前目录树对应的镜像
     */
    public FSImage getImage(long lastCheckPointMaxTxId) {
        readLock();
        try {
            if (lastCheckPointMaxTxId == this.maxTxId) {
                // 没有从元数据节点同步到事务日志
                return null;
            }

            return new FSImage(this.maxTxId, GsonUtils.toJson(this.dirTree));
        } finally {
            readUnLock();
        }
    }

    /**
     * 创建目录
     *
     * @param txId 事务标识
     * @param path 目录路径
     */
    public void mkdir(long txId, String path) {
        writeLock();
        try {
            String[] paths = path.split("/");
            INodeDirectory parent = dirTree;

            for (String childPath : paths) {
                if (childPath.trim().equals("")) {
                    continue;
                }

                INodeDirectory dir = findDirectory(parent, childPath);
                if (dir != null) {  // 目录存在
                    parent = dir;
                    continue;
                }

                INodeDirectory child = new INodeDirectory(childPath);
                parent.addChild(child);
                parent = child;
            }
        } finally {
            this.maxTxId = txId;    // 设置当前目录树对应的最大事务标识
            writeUnLock();
        }
    }

    /**
     * 创建文件<p/>
     * 1. 若创建文件已经存在，则返回false；
     * 2. 若父级目录不存在，则返回false。
     *
     * @param txId     事务标识
     * @param fileName 文件路径
     * @return 是否创建文件成功；若成功，则返回true；否则，返回false
     */
    public boolean createFile(long txId, String fileName) {
        writeLock();
        try {
            String[] paths = fileName.split("/");
            INodeDirectory parent = dirTree;

            // 遍历父级目录是否存在
            for (int i = 0; i < paths.length - 1; i++) {
                if (paths[i].trim().equals("")) {
                    continue;
                }

                INodeDirectory dir = findDirectory(parent, paths[i]);
                if (dir != null) {  // 目录存在
                    parent = dir;
                } else {
                    return false;
                }
            }

            // 创建文件是否存在
            INodeFile file = findFile(parent, paths[paths.length - 1]);
            if (file == null) {
                parent.addChild(new INodeFile(paths[paths.length - 1]));
                return true;
            }

            return false;
        } finally {
            this.maxTxId = txId;    // 设置当前目录树对应的最大事务标识
            writeUnLock();
        }
    }

    /**
     * 指定父级目录是否存在对应文件名称的文件元数据
     *
     * @param parentDir 父级目录
     * @param fileName  文件名称
     * @return 存在，则返回文件元数据；否则，返回null
     */
    private INodeFile findFile(INodeDirectory parentDir, String fileName) {
        if (parentDir == null || parentDir.getChildren().size() == 0) {
            return null;
        }

        for (INode childNode : parentDir.getChildren()) {
            if (childNode instanceof INodeFile) {
                INodeFile file = (INodeFile) childNode;

                if ((file.getName().equals(fileName))) {
                    return file;
                }
            }
        }

        return null;
    }

    /**
     * 对文件目录树递归查找目录
     *
     * @param parentDir 父级目录
     * @param childPath 子目录
     * @return 内存文件目录树中，父级目录是否存在子目录；true，存在；否则，不存在
     */
    private INodeDirectory findDirectory(INodeDirectory parentDir, String childPath) {
        if (parentDir == null || parentDir.getChildren().size() == 0) {
            return null;
        }

        for (INode childNode : parentDir.getChildren()) {
            if (childNode instanceof INodeDirectory) {
                INodeDirectory childDir = (INodeDirectory) childNode;

                if ((childDir.getPath().equals(childPath))) {
                    return childDir;
                }
            }
        }

        return null;
    }
}
