package com.sciatta.hummer.core.transport;

/**
 * Created by Rain on 2023/1/5<br>
 * All Rights Reserved(C) 2017 - 2023 SCIATTA <br> <p/>
 * 传输命令执行器
 */
public interface CommandExecutor {
    /**
     * 是否可以执行传输命令
     *
     * @param command 传输命令
     * @return true，可以执行；否则，不可执行
     */
    boolean accept(Command command);

    /**
     * 执行传输命令
     *
     * @param command 传输命令
     */
    void execute(Command command);
}
