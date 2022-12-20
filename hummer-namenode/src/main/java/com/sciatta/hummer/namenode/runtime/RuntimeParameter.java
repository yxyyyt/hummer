package com.sciatta.hummer.namenode.runtime;

/**
 * Created by Rain on 2022/12/20<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 运行时参数
 */
public class RuntimeParameter {
    /**
     * 上一次检查点的最大事务标识
     */
    public static final String LAST_CHECKPOINT_MAX_TX_ID = "lastCheckPointMaxTxId";

    /**
     * 上一次检查点完成的时间戳
     */
    public static final String LAST_CHECKPOINT_TIMESTAMP = "lastCheckPointTimestamp";
}
