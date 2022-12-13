package com.sciatta.hummer.namenode.server;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by Rain on 2022/12/13<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 管理内存中的文件目录树
 */
public class FSDirectory {
    /**
     * 内存中的文件目录树
     */
    private final INodeDirectory dirTree;

    public FSDirectory() {
        this.dirTree = new INodeDirectory("/"); // 根目录以 / 开始
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

    /**
     * 文件目录树中节点模型
     */
    private interface INode {

    }

    /**
     * 文件目录树中目录模型
     */
    public static class INodeDirectory implements INode {

        private String path;
        private List<INode> children;

        public INodeDirectory(String path) {
            this.path = path;
            this.children = new LinkedList<INode>();
        }

        public void addChild(INode inode) {
            this.children.add(inode);
        }

        public String getPath() {
            return path;
        }

        public void setPath(String path) {
            this.path = path;
        }

        public List<INode> getChildren() {
            return children;
        }

        public void setChildren(List<INode> children) {
            this.children = children;
        }

    }

    /**
     * 文件目录树中文件模型
     */
    public static class INodeFile implements INode {

        private String name;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

    }
}
