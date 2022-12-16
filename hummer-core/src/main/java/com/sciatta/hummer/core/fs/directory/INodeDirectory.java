package com.sciatta.hummer.core.fs.directory;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by Rain on 2022/12/16<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 文件目录树中目录模型
 */
public class INodeDirectory implements INode {

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
