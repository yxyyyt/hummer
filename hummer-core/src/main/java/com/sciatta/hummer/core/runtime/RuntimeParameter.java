package com.sciatta.hummer.core.runtime;

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

    /**
     * 全局事务标识
     */
    public static final String GLOBAL_TX_ID = "globalTxId";

    /**
     * 同步刷写磁盘事务标识
     */
    public static final String SYNC_LOG_TX_ID = "syncLogTxId";

    /**
     * 刷写磁盘开始事务标识
     */
    public static final String FLUSHED_START_TX_ID = "flushedStartTxId";

    /**
     * 刷写磁盘结束事务标识
     */
    public static final String FLUSHED_END_TX_ID = "flushedEndTxId";

    /**
     * 已同步到磁盘的事务日志分段
     */
    public static final String FLUSHED_SEGMENT_LIST = "flushedSegmentList";
}
