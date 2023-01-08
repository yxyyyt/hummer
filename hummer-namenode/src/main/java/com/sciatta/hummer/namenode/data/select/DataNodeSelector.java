package com.sciatta.hummer.namenode.data.select;

import com.sciatta.hummer.core.data.DataNodeInfo;

import java.util.Set;

/**
 * Created by Rain on 2022/12/31<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 数据节点选择器，获取文件所在的数据节点
 */
public interface DataNodeSelector {
    /**
     * 获取数据节点
     *
     * @param dataNodeInfos 文件所在的数据节点
     * @return 获取文件所在的数据节点的其中一个
     */
    DataNodeInfo selectDataNode(Set<DataNodeInfo> dataNodeInfos);
}
