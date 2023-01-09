package com.sciatta.hummer.datanode.server.fs;

import com.sciatta.hummer.core.server.Server;
import com.sciatta.hummer.core.transport.Command;
import com.sciatta.hummer.core.transport.CommandExecutor;
import com.sciatta.hummer.core.transport.TransportStatus;
import com.sciatta.hummer.core.util.PathUtils;
import com.sciatta.hummer.datanode.server.config.DataNodeConfig;
import com.sciatta.hummer.datanode.server.rpc.NameNodeRpcClient;
import com.sciatta.hummer.datanode.server.transport.ReRegisterCommandExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Rain on 2023/1/5<br>
 * All Rights Reserved(C) 2017 - 2023 SCIATTA <br> <p/>
 * 数据节点管理
 */
public class DataNodeManager {
    private static final Logger logger = LoggerFactory.getLogger(DataNodeManager.class);

    /**
     * 已注册的命令执行器
     */
    private final List<CommandExecutor> registeredCommandExecutors = new ArrayList<>();

    private final NameNodeRpcClient nameNodeRpcClient;
    private final Server server;

    public DataNodeManager(NameNodeRpcClient nameNodeRpcClient, Server server) {
        this.nameNodeRpcClient = nameNodeRpcClient;
        this.server = server;

        // 注册命令执行器
        registeredCommandExecutors();

        // 发起注册请求
        register();

        // 发起心跳请求
        heartbeat();
    }

    /**
     * 注册命令执行器
     */
    private void registeredCommandExecutors() {
        registeredCommandExecutors.add(new ReRegisterCommandExecutor(this));
    }

    /**
     * 向元数据节点发起注册请求
     */
    public void register() {
        int state = this.nameNodeRpcClient.register();
        if (state == TransportStatus.Register.SUCCESS) {
            // 注册成功，上报全量文件信息
            StorageInfo storageInfo = getStorageInfo();
            if (storageInfo != null) {
                state = this.nameNodeRpcClient.fullReport(storageInfo);
                if (state == TransportStatus.FullReport.FAIL) {
                    logger.error("data node close while full report storage info fail, state is {}", state);
                    server.close();
                }
                logger.debug("data node {}:{} register success and full report {}",
                        DataNodeConfig.getLocalHostname(), DataNodeConfig.getLocalPort(), storageInfo);
            }
        } else if (state == TransportStatus.Register.REGISTERED) {
            // 已注册成功，数据节点短暂重启，元数据节点尚未剔除不可用数据节点，不需要上报全部文件信息
            logger.warn("data node has registered");
        } else if (state == TransportStatus.Register.FAIL) {
            logger.error("data node close while register fail, state is {}", state);
            server.close();
        }
    }

    /**
     * 向元数据节点发起心跳请求
     */
    private void heartbeat() {
        HeartbeatThread heartbeatThread = new HeartbeatThread();
        heartbeatThread.start();
    }

    /**
     * 获取存储信息
     *
     * @return 存储信息
     */
    public StorageInfo getStorageInfo() {
        StorageInfo storageInfo = new StorageInfo();

        File dataPath = new File(DataNodeConfig.getDataNodeDataPath());
        File[] children = dataPath.listFiles();
        if (children == null || children.length == 0) {
            return null;
        }

        for (File child : children) {
            scanFiles(child, storageInfo);
        }

        return storageInfo;
    }

    /**
     * 扫描本地文件构建存储信息
     *
     * @param fileOrDir   文件或目录
     * @param storageInfo 存储信息
     */
    private void scanFiles(File fileOrDir, StorageInfo storageInfo) {
        if (fileOrDir.isFile()) {
            String path = fileOrDir.getPath();
            path = path.substring(DataNodeConfig.getDataNodeDataPath().length());   // 文件相对路径
            path = path.replace(PathUtils.getFileSeparator(), PathUtils.getINodeSeparator());   // 内存目录树文件路径

            storageInfo.addFileName(path);
            storageInfo.addStoredDataSize(fileOrDir.length());

            return;
        }

        File[] children = fileOrDir.listFiles();
        if (children == null || children.length == 0) {
            return;
        }

        for (File child : children) {
            scanFiles(child, storageInfo);
        }
    }

    /**
     * 向元数据节点发送心跳的线程
     */
    private class HeartbeatThread extends Thread {

        @Override
        public void run() {
            while (!server.isClosing()) {
                List<Command> commands = new ArrayList<>();
                int state = nameNodeRpcClient.heartbeat(commands);
                if (state == TransportStatus.HeartBeat.SUCCESS) {
                    logger.debug("data node heartbeat success, state is {}", state);
                } else if (state == TransportStatus.HeartBeat.FAIL) {
                    logger.error("data node heartbeat fail, state is {}", state);
                } else if (state == TransportStatus.HeartBeat.NOT_REGISTERED) {
                    logger.warn("data node has not registered, execute name node issued commands, state is {}", state);

                    if (commands.size() != 0) {
                        for (Command command : commands) {
                            boolean find = false;
                            for (CommandExecutor commandExecutor : registeredCommandExecutors) {
                                if (commandExecutor.accept(command)) {
                                    commandExecutor.execute(command);
                                    find = true;
                                    break;
                                }
                            }
                            if (!find) {
                                logger.warn("not any registered command executor for execute {} command", command);
                            }
                        }
                    }

                }

                try {
                    Thread.sleep(DataNodeConfig.getHeartbeatInterval());
                } catch (InterruptedException e) {
                    logger.error("{} exception while wait to send heartbeat request", e.getMessage());
                }
            }
        }
    }
}
