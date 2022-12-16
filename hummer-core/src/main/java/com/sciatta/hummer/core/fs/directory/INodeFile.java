package com.sciatta.hummer.core.fs.directory;

/**
 * Created by Rain on 2022/12/16<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 文件目录树中文件模型
 */
public class INodeFile implements INode {

    private String name;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

}
