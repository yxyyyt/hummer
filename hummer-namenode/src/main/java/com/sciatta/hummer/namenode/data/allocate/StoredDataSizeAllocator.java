package com.sciatta.hummer.namenode.data.allocate;

import com.sciatta.hummer.core.data.DataNodeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.sciatta.hummer.namenode.config.NameNodeConfig.NUMBER_OF_REPLICATED;

/**
 * Created by Rain on 2022/12/31<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 按数据节点当前存储数据的大小排序，优先选择存储数据小的节点，使得数据文件分布均匀的存储在各个数据节点上
 */
public class StoredDataSizeAllocator implements DataNodeAllocator {
    private static final Logger logger = LoggerFactory.getLogger(StoredDataSizeAllocator.class);

    @Override
    public List<DataNodeInfo> allocateDataNodes(Map<String, DataNodeInfo> aliveDataNodes) {
        List<DataNodeInfo> selectedDataNodes = new ArrayList<>(NUMBER_OF_REPLICATED);
        List<DataNodeInfo> dataNodes = new ArrayList<>(aliveDataNodes.values());

        if (dataNodes.size() < NUMBER_OF_REPLICATED) {
            logger.warn("current the number of can be allocated dataNodes {} less than NUMBER_OF_REPLICATED {}",
                    dataNodes.size(), NUMBER_OF_REPLICATED);
            return selectedDataNodes;
        }

        // 按大小排序
        dataNodes.sort((d1, d2) -> (int) (d1.getStoredDataSize() - d2.getStoredDataSize()));

        for (int i = 0; i < NUMBER_OF_REPLICATED; i++) {
            selectedDataNodes.add(dataNodes.get(i));
        }

        return selectedDataNodes;
    }
}
