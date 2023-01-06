package com.sciatta.hummer.core.transport;

/**
 * Created by Rain on 2023/1/5<br>
 * All Rights Reserved(C) 2017 - 2023 SCIATTA <br> <p/>
 * 传输状态
 */
public final class TransportStatus {

    /**
     * 公共状态
     */
    private static class Common {
        /**
         * 成功
         */
        public static final int SUCCESS = 0;
        /**
         * 失败
         */
        public static final int FAIL = 1;
    }

    /**
     * 注册状态
     */
    public static final class Register extends Common {
        /**
         * 已注册
         */
        public static final int REGISTERED = 100;
    }

    /**
     * 心跳状态
     */
    public static final class HeartBeat extends Common {
        /**
         * 未注册
         */
        public static final int NOT_REGISTERED = 100;

        /**
         * 命令类型
         */
        public static class CommandType {
            /**
             * 未注册，需要重新注册
             */
            public static final int RE_REGISTER = 0;
        }
    }

    /**
     * 创建目录状态
     */
    public static final class Mkdir extends Common {

    }

    /**
     * 创建文件状态
     */
    public static final class CreateFile extends Common {

    }

    /**
     * 增量上报状态
     */
    public static final class IncrementalReport extends Common {

    }

    /**
     * 全量上报状态
     */
    public static final class FullReport extends Common {
    }

    /**
     * 事务日志同步状态
     */
    public static final class FetchEditsLog extends Common {

    }

    /**
     * 分配数据节点状态
     */
    public static final class AllocateDataNodes extends Common {

    }

    /**
     * 停机状态
     */
    public static final class Shutdown extends Common {

    }


}
