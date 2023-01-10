package com.sciatta.hummer.datanode.server.transport;

import com.sciatta.hummer.core.transport.Command;
import com.sciatta.hummer.core.transport.CommandExecutor;
import com.sciatta.hummer.core.transport.ReplicateTaskCommand;
import com.sciatta.hummer.core.transport.TransportStatus;
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
 * 复制副本任务命令执行器
 */
public class ReplicateTaskCommandExecutor implements CommandExecutor {
    private static final Logger logger = LoggerFactory.getLogger(ReplicateTaskCommandExecutor.class);

    private final DataNodeFileClient dataNodeFileClient;
    private final NameNodeRpcClient nameNodeRpcClient;

    public ReplicateTaskCommandExecutor(DataNodeFileClient dataNodeFileClient, NameNodeRpcClient nameNodeRpcClient) {
        this.dataNodeFileClient = dataNodeFileClient;
        this.nameNodeRpcClient = nameNodeRpcClient;
    }

    @Override
    public boolean accept(Command command) {
        return command.getType() == TransportStatus.HeartBeat.CommandType.REPLICATE;
    }

    @Override
    public void execute(Command command) {
        ReplicateTaskCommand replicateTaskCommand = (ReplicateTaskCommand) command;

        byte[] bytes = this.dataNodeFileClient.downloadFile(
                replicateTaskCommand.getHostname(),
                replicateTaskCommand.getPort(),
                replicateTaskCommand.getFileName()
        );

        try {
            String absoluteFileName = PathUtils.getAbsoluteFileName(DataNodeConfig.getDataNodeDataPath(),
                    replicateTaskCommand.getFileName());

            // 保存文件
            Files.write(Paths.get(absoluteFileName), bytes);
            logger.debug("from {}:{} data node download file {}, write {} bytes to {}",
                    replicateTaskCommand.getHostname(),
                    replicateTaskCommand.getPort(),
                    replicateTaskCommand.getFileName(),
                    bytes.length,
                    absoluteFileName
            );

            // 向元数据节点增量上报文件
            this.nameNodeRpcClient.incrementalReport(replicateTaskCommand.getFileName(), bytes.length);
            logger.debug("incremental report file name is {} , file size is {}",
                    replicateTaskCommand.getFileName(), bytes.length);

        } catch (IOException e) {
            logger.error("{} while from {}:{} data node download file {}",
                    e.getMessage(),
                    replicateTaskCommand.getHostname(),
                    replicateTaskCommand.getPort(),
                    replicateTaskCommand.getFileName()
            );
        }
    }
}
