package com.sciatta.hummer.core.server;

import java.io.IOException;

/**
 * Created by Rain on 2022/12/16<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 服务
 */
public interface Server {
    /**
     * 启动服务
     *
     * @return 服务
     * @throws IOException IO异常
     */
    Server start() throws IOException;

    /**
     * 关闭服务
     */
    void close();

    /**
     * 阻塞等待服务
     */
    void keep();

    /**
     * 是否启动成功
     *
     * @return true，启动成功；否则，未启动成功
     */
    boolean isStarted();

    /**
     * 是否正在关闭
     *
     * @return true，正在关闭；否则，未正在关闭
     */
    boolean isClosing();
}
