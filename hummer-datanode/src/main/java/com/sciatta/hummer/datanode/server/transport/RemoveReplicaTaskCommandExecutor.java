package com.sciatta.hummer.datanode.server.transport;

import com.sciatta.hummer.core.transport.*;
import com.sciatta.hummer.core.util.PathUtils;
import com.sciatta.hummer.datanode.server.config.DataNodeConfig;
import com.sciatta.hummer.datanode.server.fs.DataNodeFileClient;
import com.sciatta.hummer.datanode.server.rpc.NameNodeRpcClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Created by Rain on 2023/1/9<br>
 * All Rights Reserved(C) 2017 - 2023 SCIATTA <br> <p/>
 * 删除副本任务命令执行器
 */
public class RemoveReplicaTaskCommandExecutor implements CommandExecutor {
    private static final Logger logger = LoggerFactory.getLogger(RemoveReplicaTaskCommandExecutor.class);

    @Override
    public boolean accept(Command command) {
        return command.getType() == TransportStatus.HeartBeat.CommandType.REMOVE_REPLICA;
    }

    @Override
    public void execute(Command command) {
        RemoveReplicaTaskCommand removeReplicaTaskCommand = (RemoveReplicaTaskCommand) command;

        try {
            String absoluteFileName = PathUtils.getAbsoluteFileName(DataNodeConfig.getDataNodeDataPath(),
                    removeReplicaTaskCommand.getFileName());

            // 删除文件
            Files.deleteIfExists(Paths.get(absoluteFileName));

            logger.debug("remove redundant replica {} from local {}",
                    removeReplicaTaskCommand.getFileName(),
                    absoluteFileName);

        } catch (IOException e) {
            logger.error("{} while remove redundant replica {}", e.getMessage(), removeReplicaTaskCommand.getFileName());
        }
    }
}
