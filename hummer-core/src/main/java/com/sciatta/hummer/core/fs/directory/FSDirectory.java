package com.sciatta.hummer.core.fs.directory;

/**
 * Created by Rain on 2022/12/13<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 管理内存中的文件目录树
 */
public class FSDirectory {

    private final INodeDirectory dirTree;

    public FSDirectory() {
        this.dirTree = new INodeDirectory("/");
    }

    /**
     * 创建目录
     *
     * @param path 目录路径
     */
    public void mkdir(String path) {
        synchronized (dirTree) {
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
            }
        }
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
