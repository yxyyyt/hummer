package com.sciatta.hummer.namenode.fs.allocate;

import com.sciatta.hummer.core.fs.DataNodeInfo;
import com.sciatta.hummer.namenode.config.NameNodeConfig;

import java.util.List;
import java.util.Map;

/**
 * Created by Rain on 2022/12/31<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 数据节点分配器，为上传文件分配可用数据节点 {@link NameNodeConfig#getNumberOfReplicated()}
 */
public interface DataNodeAllocator {
    /**
     * 分配数据节点
     *
     * @param availableDataNodes 待分配数据节点
     * @return 已成功分配的数据节点
     */
    List<DataNodeInfo> allocateDataNodes(Map<String, DataNodeInfo> availableDataNodes);
}
