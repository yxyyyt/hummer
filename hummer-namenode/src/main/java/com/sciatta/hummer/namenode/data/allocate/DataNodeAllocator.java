package com.sciatta.hummer.namenode.data.allocate;

import com.sciatta.hummer.core.data.DataNodeInfo;

import java.util.List;
import java.util.Map;

/**
 * Created by Rain on 2022/12/31<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 数据节点分配器
 */
public interface DataNodeAllocator {
    /**
     * 分配数据节点
     *
     * @param aliveDataNodes 待分配数据节点
     * @return 已成功分配的数据节点
     */
    List<DataNodeInfo> allocateDataNodes(Map<String, DataNodeInfo> aliveDataNodes);
}
