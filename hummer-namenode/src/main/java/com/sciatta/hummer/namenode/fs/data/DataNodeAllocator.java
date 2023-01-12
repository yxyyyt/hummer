package com.sciatta.hummer.namenode.fs.data;

import com.sciatta.hummer.core.fs.data.DataNodeInfo;
import com.sciatta.hummer.namenode.config.NameNodeConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by Rain on 2022/12/31<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 数据节点分配器
 */
public class DataNodeAllocator {
    private static final Logger logger = LoggerFactory.getLogger(DataNodeAllocator.class);

    private final DataNodeManager dataNodeManager;

    private final Random random = new Random();

    public DataNodeAllocator(DataNodeManager dataNodeManager) {
        this.dataNodeManager = dataNodeManager;
    }


    /**
     * 为上传文件分配可用数据节点 {@link NameNodeConfig#getNumberOfReplicated()}
     *
     * @return 已成功分配的数据节点，数据节点已经累加文件大小
     */
    public List<DataNodeInfo> allocateDataNodes() {
        List<DataNodeInfo> selectedDataNodes = new ArrayList<>(NameNodeConfig.getNumberOfReplicated());
        List<DataNodeInfo> dataNodes = new ArrayList<>(this.dataNodeManager.getAvailableDataNodes().values());

        if (dataNodes.size() < NameNodeConfig.getNumberOfReplicated()) {
            logger.warn("current the number of can be allocated dataNodes {} less than NUMBER_OF_REPLICATED {}",
                    dataNodes.size(), NameNodeConfig.getNumberOfReplicated());
            return null;
        }

        // 按数据节点当前存储数据的大小排序，优先选择存储数据小的节点，使得数据文件分布均匀的存储在各个数据节点上
        dataNodes.sort((d1, d2) -> (int) (d1.getStoredDataSize() - d2.getStoredDataSize()));

        for (int i = 0; i < NameNodeConfig.getNumberOfReplicated(); i++) {
            selectedDataNodes.add(dataNodes.get(i));
        }

        return selectedDataNodes;
    }

    /**
     * 获取文件所在数据节点的其中一个可用数据节点
     *
     * @param fileName 文件名
     * @return 文件所在数据节点的其中一个可以数据节点
     */
    public DataNodeInfo selectOneDataNodeForFile(String fileName) {
        Set<DataNodeInfo> dataNodeInfos = this.dataNodeManager.getFileNameToDataNodeCache().get(fileName);
        if (dataNodeInfos == null || dataNodeInfos.size() == 0) {
            return null;
        }

        // 随机获取文件所在的数据节点
        int index = random.nextInt(dataNodeInfos.size());

        return (DataNodeInfo) dataNodeInfos.toArray()[index];
    }

    /**
     * 分配源复制数据节点
     *
     * @param fileName            文件名
     * @param unavailableDataNode 不可用数据节点
     * @return 源复制数据节点；如果不存在，返回null
     */
    public DataNodeInfo allocatorReplicateSource(String fileName, DataNodeInfo unavailableDataNode) {
        Set<DataNodeInfo> dataNodeInfos = this.dataNodeManager.getFileNameToDataNodeCache().get(fileName);
        if (dataNodeInfos == null || dataNodeInfos.size() == 0) {
            return null;
        }

        Set<DataNodeInfo> helpDataNodeInfos = new HashSet<>(dataNodeInfos);
        helpDataNodeInfos.remove(unavailableDataNode);

        if (helpDataNodeInfos.size() == 0) {
            return null;
        }

        // 随机获取文件所在的数据节点
        int index = random.nextInt(helpDataNodeInfos.size());
        DataNodeInfo source = (DataNodeInfo) helpDataNodeInfos.toArray()[index];

        logger.debug("{} has data nodes {}, unavailable data node {}, allocator source {}",
                fileName, dataNodeInfos, unavailableDataNode, source);
        return source;
    }

    /**
     * 分配目的复制数据节点
     *
     * @param fileName 文件名
     * @return 目的复制数据节点；如果不存在，返回null
     */
    public DataNodeInfo allocatorReplicateDestination(String fileName) {
        Set<DataNodeInfo> dataNodeInfos = this.dataNodeManager.getFileNameToDataNodeCache().get(fileName);
        DataNodeInfo destination = null;

        try {
            if (dataNodeInfos == null || dataNodeInfos.size() == 0) {
                return null;
            }

            List<DataNodeInfo> helpDataNodeInfos = new ArrayList<>(this.dataNodeManager.getAvailableDataNodes().values());
            helpDataNodeInfos.removeAll(dataNodeInfos);

            if (helpDataNodeInfos.size() <= 0) {
                return null;
            }

            if (helpDataNodeInfos.size() == 1) {
                destination = helpDataNodeInfos.get(0);
                return destination;
            }

            // 按数据节点当前存储数据的大小排序，优先选择存储数据小的节点，使得数据文件分布均匀的存储在各个数据节点上
            helpDataNodeInfos.sort((d1, d2) -> (int) (d1.getStoredDataSize() - d2.getStoredDataSize()));

            destination = helpDataNodeInfos.get(0);
            return destination;
        } finally {
            logger.debug("{} has data nodes {}, allocator destination {}",
                    fileName, dataNodeInfos, destination);
        }

    }
}
