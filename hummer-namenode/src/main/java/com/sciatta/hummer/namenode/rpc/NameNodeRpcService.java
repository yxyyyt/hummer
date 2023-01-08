package com.sciatta.hummer.namenode.rpc;

import com.google.gson.reflect.TypeToken;
import com.sciatta.hummer.core.data.DataNodeInfo;
import com.sciatta.hummer.core.fs.editlog.EditLog;
import com.sciatta.hummer.core.fs.editlog.FlushedSegment;
import com.sciatta.hummer.core.server.Server;
import com.sciatta.hummer.core.transport.Command;
import com.sciatta.hummer.core.transport.TransportStatus;
import com.sciatta.hummer.core.util.GsonUtils;
import com.sciatta.hummer.namenode.config.NameNodeConfig;
import com.sciatta.hummer.namenode.data.DataNodeManager;
import com.sciatta.hummer.namenode.fs.FSNameSystem;
import com.sciatta.hummer.rpc.*;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by Rain on 2022/12/13<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 元数据节点对外提供RPC服务接口，使用Grpc实现
 */
public class NameNodeRpcService extends NameNodeServiceGrpc.NameNodeServiceImplBase {

    private static final Logger logger = LoggerFactory.getLogger(NameNodeRpcService.class);

    private final FSNameSystem fsNameSystem;
    private final DataNodeManager dataNodeManager;
    private final Server server;

    //-------------------------------------
    // TODO 重构 请求处理分发器
    /**
     * 当前缓存的一批事务日志中最大的事务日志的事务标识
     */
    private long localBufferedEditsLogMaxTxId;

    /**
     * 当前已被缓存的磁盘事务日志分段
     */
    private FlushedSegment localBufferedFlushedSegment;

    /**
     * 当前缓存的一批供拉取的事务日志
     */
    private final Deque<EditLog> localBufferedEditsLog = new LinkedList<>();
    //-------------------------------------

    public NameNodeRpcService(FSNameSystem fsNameSystem, DataNodeManager dataNodeManager, Server server) {
        this.fsNameSystem = fsNameSystem;
        this.dataNodeManager = dataNodeManager;
        this.server = server;
    }

    @Override   // TODO 重构 请求处理分发器
    public void register(RegisterRequest request, StreamObserver<RegisterResponse> responseObserver) {
        RegisterResponse response;

        if (!server.isStarted() || server.isClosing()) {
            response = RegisterResponse.newBuilder().setStatus(TransportStatus.Register.FAIL).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            return;
        }

        int state = dataNodeManager.register(request.getHostname(), request.getPort());
        response = RegisterResponse.newBuilder().setStatus(state).build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void heartbeat(HeartbeatRequest request, StreamObserver<HeartbeatResponse> responseObserver) {
        HeartbeatResponse response;

        if (!server.isStarted() || server.isClosing()) {
            response = HeartbeatResponse.newBuilder().setStatus(TransportStatus.HeartBeat.FAIL).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            return;
        }

        List<Command> commands = new ArrayList<>();
        int state = dataNodeManager.heartbeat(request.getHostname(), request.getPort());
        if (state == TransportStatus.HeartBeat.NOT_REGISTERED) {
            commands.add(new Command(TransportStatus.HeartBeat.CommandType.RE_REGISTER));
        }

        response = HeartbeatResponse.newBuilder()
                .setStatus(state)
                .setRemoteCommands(GsonUtils.toJson(commands)).build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void mkdir(MkdirRequest request, StreamObserver<MkdirResponse> responseObserver) {
        MkdirResponse response;

        if (!server.isStarted() || server.isClosing()) {
            response = MkdirResponse.newBuilder().setStatus(TransportStatus.Mkdir.FAIL).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            return;
        }

        boolean test = fsNameSystem.mkdir(request.getPath());

        if (test) {
            response = MkdirResponse.newBuilder().setStatus(TransportStatus.Mkdir.SUCCESS).build();
        } else {
            response = MkdirResponse.newBuilder().setStatus(TransportStatus.Mkdir.FAIL).build();
        }

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void createFile(CreateFileRequest request, StreamObserver<CreateFileResponse> responseObserver) {
        CreateFileResponse response;

        if (!server.isStarted() || server.isClosing()) {
            response = CreateFileResponse.newBuilder().setStatus(TransportStatus.CreateFile.FAIL).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            return;
        }

        boolean test = fsNameSystem.createFile(request.getFileName());

        if (test) {
            response = CreateFileResponse.newBuilder().setStatus(TransportStatus.CreateFile.SUCCESS).build();
        } else {
            response = CreateFileResponse.newBuilder().setStatus(TransportStatus.CreateFile.FAIL).build();
        }

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void incrementalReport(IncrementalReportRequest request, StreamObserver<IncrementalReportResponse> responseObserver) {
        IncrementalReportResponse response;

        if (!server.isStarted() || server.isClosing()) {
            response = IncrementalReportResponse.newBuilder().setStatus(TransportStatus.IncrementalReport.FAIL).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            return;
        }

        boolean test = dataNodeManager.incrementalReport(request.getHostname(), request.getPort(), request.getFileName());

        if (test) {
            response = IncrementalReportResponse.newBuilder().setStatus(TransportStatus.IncrementalReport.SUCCESS).build();
            logger.debug("{} incremental report to data node {}:{} success",
                    request.getFileName(), request.getHostname(), request.getPort());
        } else {
            response = IncrementalReportResponse.newBuilder().setStatus(TransportStatus.IncrementalReport.FAIL).build();
            logger.debug("{} incremental report to data node {}:{} fail",
                    request.getFileName(), request.getHostname(), request.getPort());
        }

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void fullReport(FullReportRequest request, StreamObserver<FullReportResponse> responseObserver) {
        FullReportResponse response;

        if (!server.isStarted() || server.isClosing()) {
            response = FullReportResponse.newBuilder().setStatus(TransportStatus.FullReport.FAIL).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            return;
        }

        boolean test = dataNodeManager.fullReport(request.getHostname(),
                request.getPort(),
                GsonUtils.fromJson(request.getFileNames(), new TypeToken<List<String>>() {
                }.getType()),
                request.getStoredDataSize());

        if (test) {
            response = FullReportResponse.newBuilder().setStatus(TransportStatus.FullReport.SUCCESS).build();
            logger.debug("{} full report to data node {}:{} success",
                    request.getFileNames(), request.getHostname(), request.getPort());
        } else {
            response = FullReportResponse.newBuilder().setStatus(TransportStatus.FullReport.FAIL).build();
            logger.debug("{} incremental report to data node {}:{} fail",
                    request.getFileNames(), request.getHostname(), request.getPort());
        }

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void fetchEditsLog(FetchEditsLogRequest request, StreamObserver<FetchEditsLogResponse> responseObserver) {
        FetchEditsLogResponse response;

        if (!server.isStarted() || server.isClosing()) {
            response = FetchEditsLogResponse.newBuilder().setStatus(TransportStatus.FetchEditsLog.FAIL).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            return;
        }

        List<EditLog> editsLog = doFetchEditsLog(request.getSyncedTxId());

        response = FetchEditsLogResponse.newBuilder().setStatus(TransportStatus.FetchEditsLog.SUCCESS).setEditsLog(GsonUtils.toJson(editsLog)).build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void allocateDataNodes(AllocateDataNodesRequest request, StreamObserver<AllocateDataNodesResponse> responseObserver) {
        AllocateDataNodesResponse response;

        if (!server.isStarted() || server.isClosing()) {
            response = AllocateDataNodesResponse.newBuilder().setStatus(TransportStatus.AllocateDataNodes.FAIL).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            return;
        }

        List<DataNodeInfo> dataNodes = this.dataNodeManager.allocateDataNodes(request.getFileSize());
        if (dataNodes.size() > 0) {
            response = AllocateDataNodesResponse.newBuilder()
                    .setStatus(TransportStatus.AllocateDataNodes.SUCCESS)
                    .setDataNodes(GsonUtils.toJson(dataNodes))
                    .build();
        } else {
            response = AllocateDataNodesResponse.newBuilder().setStatus(TransportStatus.AllocateDataNodes.FAIL).build();
        }

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void shutdown(ShutdownRequest request, StreamObserver<ShutdownResponse> responseObserver) {
        ShutdownResponse response;

        if (!server.isStarted() || server.isClosing()) {
            response = ShutdownResponse.newBuilder().setStatus(TransportStatus.Shutdown.FAIL).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            return;
        }

        server.close();

        response = ShutdownResponse.newBuilder().setStatus(TransportStatus.Shutdown.SUCCESS).build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void getDataNodeForFile(GetDataNodeForFileRequest request, StreamObserver<GetDataNodeForFileResponse> responseObserver) {
        GetDataNodeForFileResponse response;

        if (!server.isStarted() || server.isClosing()) {
            response = GetDataNodeForFileResponse.newBuilder().setStatus(TransportStatus.GetDataNodeForFile.FAIL).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            return;
        }

        DataNodeInfo dataNodeForFile = this.dataNodeManager.getDataNodeForFile(request.getFileName());

        if (dataNodeForFile == null) {
            response = GetDataNodeForFileResponse.newBuilder().setStatus(TransportStatus.GetDataNodeForFile.FAIL).build();
        } else {
            response = GetDataNodeForFileResponse.newBuilder()
                    .setDataNodeInfo(GsonUtils.toJson(dataNodeForFile))
                    .setStatus(TransportStatus.GetDataNodeForFile.SUCCESS)
                    .build();
        }

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    // TODO 重构 请求处理分发器

    /**
     * 备份节点从元数据节点同步事务日志
     *
     * @param syncedTxId 上一次同步到的事务日志的事务标识
     * @return 待同步的事务日志
     */
    private List<EditLog> doFetchEditsLog(long syncedTxId) {
        logger.debug("last synced txId is {}", syncedTxId);

        List<EditLog> fetchedEditsLog = new ArrayList<>();
        List<FlushedSegment> flushedSegments = fsNameSystem.getFlushedSegments();

        logger.debug("flushed segments size is {}", flushedSegments.size());

        // 优先判断是否已经刷写磁盘
        if (flushedSegments.size() == 0) {
            fetchFromBuffer(syncedTxId, fetchedEditsLog);  // 如果没有刷写磁盘，直接从内存中获取
        } else {
            fetchFromFlushedFile(syncedTxId, flushedSegments, fetchedEditsLog);   // 从磁盘文件获取
        }

        return fetchedEditsLog;
    }

    /**
     * 从内存中获取事务日志
     *
     * @param syncedTxId      已同步的事务标识
     * @param fetchedEditsLog 获取到的事务日志
     */
    private void fetchFromBuffer(long syncedTxId, List<EditLog> fetchedEditsLog) {
        // local buffer中存在没有拉取的数据，从local buffer中拉取
        if (syncedTxId + 1 <= this.localBufferedEditsLogMaxTxId) {
            fetchFromLocalBuffer(syncedTxId, fetchedEditsLog);
            return;
        }

        // local buffer中没有待拉取的数据，从double buffer中拉取数据到local buffer中，再从local buffer中拉取
        fetchFromDoubleBuffer(syncedTxId);
        fetchFromLocalBuffer(syncedTxId, fetchedEditsLog);
    }

    /**
     * 从本地缓存中获取事务日志
     *
     * @param syncedTxId      已同步的事务标识
     * @param fetchedEditsLog 获取到的事务日志
     */
    private void fetchFromLocalBuffer(long syncedTxId, List<EditLog> fetchedEditsLog) {
        logger.debug("current synced txId is {}, local buffered editsLog max txId is {}, fetch from local buffer",
                syncedTxId, localBufferedEditsLogMaxTxId);

        int fetchCount = 0;
        for (EditLog editLog : this.localBufferedEditsLog) {
            if (editLog.getTxId() >= syncedTxId + 1) { // txId可能会中断

                if (editLog.getTxId() > syncedTxId + 1) {
                    logger.warn("current editLog txId {} > next synced txId {}, txId interruption occurred",
                            editLog.getTxId(), syncedTxId + 1);
                }

                fetchedEditsLog.add(editLog);
                syncedTxId = editLog.getTxId();
                fetchCount++;
            }

            if (fetchCount == NameNodeConfig.getBackupNodeMaxFetchSize()) { // TODO 防御编程，大小还是备份节点上传，这里不能超过这个最大值
                break;
            }
        }

        logger.debug("current synced txId is {}, local buffered editsLog max txId is {}, fetch {} editsLog from local buffer",
                syncedTxId, localBufferedEditsLogMaxTxId, fetchedEditsLog.size());
    }

    /**
     * 从双缓存中拉取数据到本地缓存，需要获取锁，有性能损耗
     *
     * @param syncedTxId 已同步的事务标识
     */
    private void fetchFromDoubleBuffer(long syncedTxId) {
        logger.debug("current synced txId is {}, local buffered editsLog max txId is {}, fetch from double buffer",
                syncedTxId, localBufferedEditsLogMaxTxId);

        cleanCache();

        this.localBufferedEditsLog.addAll(this.fsNameSystem.getEditsLogFromDoubleBuffer());

        if (this.localBufferedEditsLog.size() > 0) {
            this.localBufferedEditsLogMaxTxId = this.localBufferedEditsLog.getLast().getTxId();
        }

        logger.debug("current synced txId is {}, local buffered editsLog max txId is {}, fetch {} editsLog from double buffer",
                syncedTxId, localBufferedEditsLogMaxTxId, localBufferedEditsLog.size());
    }

    /**
     * 从磁盘文件获取事务日志
     *
     * @param syncedTxId      已同步的事务标识
     * @param flushedSegments 已刷盘事务日志文件的事务日志分段
     * @param fetchedEditsLog 获取到的事务日志
     */
    private void fetchFromFlushedFile(long syncedTxId, List<FlushedSegment> flushedSegments, List<EditLog> fetchedEditsLog) {
        if (this.localBufferedFlushedSegment != null) {
            // 磁盘文件被缓存过
            // 在 local buffer 中存在
            if (existInFlushedFile(syncedTxId, this.localBufferedFlushedSegment)) {
                logger.debug("local buffered flushed segment is {}, current synced txId is {}, exist in flushed file buffer",
                        this.localBufferedFlushedSegment, syncedTxId);
                fetchFromLocalBuffer(syncedTxId, fetchedEditsLog);
                return;
            }

            FlushedSegment nextFlushedSegment = getNextFlushedSegment(this.localBufferedFlushedSegment, flushedSegments);
            // 如果可以找到下一个磁盘文件，那么就从下一个磁盘文件里开始读取数据
            if (nextFlushedSegment != null) {
                logger.debug("next flushed file segment is {}, current synced txId is {}, exist in next flushed file",
                        nextFlushedSegment, syncedTxId);
                fetchFromFlushedFile(syncedTxId, nextFlushedSegment);
                fetchFromLocalBuffer(syncedTxId, fetchedEditsLog);
            } else {
                logger.debug("not exist in next flushed file, get from buffer");
                fetchFromBuffer(syncedTxId, fetchedEditsLog);
            }
        } else {
            // 没有缓存磁盘文件
            // 遍历所有刷写的磁盘文件，如果找到满足的，则加载磁盘文件到local buffer，再从local buffer中拉取
            logger.debug("not exit in local buffered flushed txId, fetch it from all flushed files");
            for (FlushedSegment flushedSegment : flushedSegments) {
                if (existInFlushedFile(syncedTxId, flushedSegment)) {
                    logger.debug("accepted flushed file segment is {}, current synced txId is {}, exist in flushed file",
                            flushedSegment, syncedTxId);
                    fetchFromFlushedFile(syncedTxId, flushedSegment);
                    fetchFromLocalBuffer(syncedTxId, fetchedEditsLog);
                    return;
                }
            }

            // 所有磁盘文件都不存在，从内存中加载
            logger.debug("not exist in all flushed files, fetch from buffer");
            fetchFromBuffer(syncedTxId, fetchedEditsLog);
        }
    }

    /**
     * 通过给出的事务日志分度，加载指定的磁盘文件到本地缓存，需要读磁盘文件，有性能损耗
     *
     * @param syncedTxId     已同步的事务标识
     * @param flushedSegment 已刷盘事务日志文件的事务日志分段
     */
    private void fetchFromFlushedFile(long syncedTxId, FlushedSegment flushedSegment) {
        logger.debug("current synced txId is {}, local buffered editsLog max txId is {}, fetch from flushed file segment is {}",
                syncedTxId, localBufferedEditsLogMaxTxId, flushedSegment);

        cleanCache();

        this.localBufferedEditsLog.addAll(this.fsNameSystem.getEditsLogFromFlushedFile(flushedSegment));
        EditLog lastEditLog = this.localBufferedEditsLog.getLast();
        if (lastEditLog != null) {
            this.localBufferedEditsLogMaxTxId = lastEditLog.getTxId();
        }

        this.localBufferedFlushedSegment = flushedSegment; // 当前磁盘文件数据已经被缓存

        logger.debug("current synced txId is {}, local buffered editsLog max txId is {}, fetch {} editsLog from flushed file segment is {}",
                syncedTxId, localBufferedEditsLogMaxTxId, localBufferedEditsLog.size(), flushedSegment);

    }

    /**
     * 当前拉取的事务标识是否存在给出的事务日志分段中
     *
     * @param syncedTxId     已同步的事务标识
     * @param flushedSegment 事务日志分段
     * @return true，在事务日志分段中存在；否则，不存在
     */
    private boolean existInFlushedFile(long syncedTxId, FlushedSegment flushedSegment) {
        long fetchTxId = syncedTxId + 1;
        return fetchTxId >= flushedSegment.getMinTxId() && fetchTxId <= flushedSegment.getMaxTxId();
    }

    /**
     * 获取当前缓存的磁盘文件的下一个事务日志分段
     *
     * @param localBufferedFlushedSegment 当前缓存的磁盘文件事务分段
     * @param flushedSegments             所有磁盘文件的事务分段
     * @return 当前缓存的事务日志分段的下一个磁盘文件事务日志分段；如果不存在，返回null
     */
    private FlushedSegment getNextFlushedSegment(FlushedSegment localBufferedFlushedSegment, List<FlushedSegment> flushedSegments) {

        for (int i = 0; i < flushedSegments.size(); i++) {
            if (flushedSegments.get(i) == localBufferedFlushedSegment && (i + 1 < flushedSegments.size())) {
                return flushedSegments.get(i + 1);
            }
        }
        return null;
    }

    /**
     * 清空本地缓存
     */
    private void cleanCache() {
        this.localBufferedFlushedSegment = null;
        this.localBufferedEditsLog.clear();
    }
}
