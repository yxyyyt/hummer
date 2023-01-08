package com.sciatta.hummer.namenode.data.select.impl;

import com.sciatta.hummer.core.data.DataNodeInfo;
import com.sciatta.hummer.namenode.data.select.DataNodeSelector;

import java.util.Random;
import java.util.Set;

/**
 * Created by Rain on 2023/1/7<br>
 * All Rights Reserved(C) 2017 - 2023 SCIATTA <br> <p/>
 * 随机获取文件所在的数据节点
 */
public class RandomDataNodeSelector implements DataNodeSelector {
    private final Random random = new Random();

    @Override
    public DataNodeInfo selectDataNode(Set<DataNodeInfo> dataNodeInfos) {
        int index = random.nextInt(dataNodeInfos.size());

        return (DataNodeInfo) dataNodeInfos.toArray()[index];
    }
}
