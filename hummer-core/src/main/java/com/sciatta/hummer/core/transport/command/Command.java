package com.sciatta.hummer.core.transport.command;

/**
 * Created by Rain on 2023/1/5<br>
 * All Rights Reserved(C) 2017 - 2023 SCIATTA <br> <p/>
 * 传输命令
 */
public class Command {
    /**
     * 命令类型
     */
    protected int type;

    public Command(int type) {
        this.type = type;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return "Command{" +
                "type=" + type +
                '}';
    }
}
