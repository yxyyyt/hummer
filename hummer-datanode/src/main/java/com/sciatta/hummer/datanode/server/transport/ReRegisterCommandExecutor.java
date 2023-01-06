package com.sciatta.hummer.datanode.server.transport;

import com.sciatta.hummer.core.transport.Command;
import com.sciatta.hummer.core.transport.CommandExecutor;
import com.sciatta.hummer.core.transport.TransportStatus;
import com.sciatta.hummer.datanode.server.data.DataNodeManager;

/**
 * Created by Rain on 2023/1/5<br>
 * All Rights Reserved(C) 2017 - 2023 SCIATTA <br> <p/>
 * 重新注册命令执行器
 */
public class ReRegisterCommandExecutor implements CommandExecutor {
    private final DataNodeManager dataNodeManager;

    public ReRegisterCommandExecutor(DataNodeManager dataNodeManager) {
        this.dataNodeManager = dataNodeManager;
    }

    @Override
    public boolean accept(Command command) {
        return command.getType() == TransportStatus.HeartBeat.CommandType.RE_REGISTER;
    }

    @Override
    public void execute(Command command) {
        this.dataNodeManager.register();
    }
}
