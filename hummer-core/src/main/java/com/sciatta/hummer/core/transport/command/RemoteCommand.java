package com.sciatta.hummer.core.transport.command;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Rain on 2023/1/12<br>
 * All Rights Reserved(C) 2017 - 2023 SCIATTA <br> <p/>
 * 远程命令
 */
public class RemoteCommand {
    /**
     * 命令集合
     */
    private List<Command> commands;

    public RemoteCommand() {
        this.commands = new ArrayList<>();
    }

    public List<Command> getCommands() {
        return commands;
    }

    public void setCommands(List<Command> commands) {
        this.commands = commands;
    }

    public void addCommand(Command command) {
        this.commands.add(command);
    }

    @Override
    public String toString() {
        return "RemoteCommand{" +
                "commands=" + commands +
                '}';
    }
}
