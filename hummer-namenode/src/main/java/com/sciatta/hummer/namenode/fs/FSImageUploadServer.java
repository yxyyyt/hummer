package com.sciatta.hummer.namenode.fs;

import com.sciatta.hummer.core.fs.AbstractFSNameSystem;
import com.sciatta.hummer.core.server.Server;
import com.sciatta.hummer.core.util.PathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;

import static com.sciatta.hummer.core.runtime.RuntimeParameter.LAST_CHECKPOINT_MAX_TX_ID;
import static com.sciatta.hummer.core.runtime.RuntimeParameter.LAST_CHECKPOINT_TIMESTAMP;
import static com.sciatta.hummer.namenode.config.NameNodeConfig.CHECKPOINT_PATH;
import static com.sciatta.hummer.namenode.config.NameNodeConfig.NAME_NODE_IMAGE_UPLOAD_SERVER_PORT;

/**
 * Created by Rain on 2022/10/25<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 接收备份节点上传镜像请求
 */
public class FSImageUploadServer extends Thread {

    private static final Logger logger = LoggerFactory.getLogger(FSImageUploadServer.class);

    private Selector selector;
    private ServerSocketChannel serverSocketChannel;

    private final Server server;
    private final AbstractFSNameSystem fsNameSystem;

    /**
     * 缓存检查点的最大事务标识
     */
    private ByteBuffer cachedCheckPointMaxTxId;
    /**
     * 缓存检查点完成的时间戳
     */
    private ByteBuffer cachedCheckPointTimestamp;
    /**
     * 缓存目录镜像长度
     */
    private ByteBuffer cachedDirectoryLength;
    /**
     * 缓存目录镜像
     */
    private ByteBuffer cachedDirectory;

    /**
     * 已解析完成的镜像报文缓存
     */
    private final CachedFSImage cachedFSImage = new CachedFSImage();

    /**
     * 缓存完整镜像报文帮助类
     */
    private static class CachedFSImage {
        long checkPointMaxTxId;
        long checkPointTimestamp;
        int directoryLength;
        ByteBuffer directory;

        public void clear() {
            checkPointMaxTxId = 0;
            checkPointTimestamp = 0;
            directoryLength = 0;
            directory.clear();
            directory = null;
        }
    }

    public FSImageUploadServer(AbstractFSNameSystem fsNameSystem, Server server) {
        this.fsNameSystem = fsNameSystem;
        this.server = server;
    }

    @Override
    public void run() {
        try {
            initServerSocketChannel();

            process();
        } catch (IOException e) {
            logger.error("{} while start image upload server", e.getMessage());
        } finally {
            if (serverSocketChannel != null) {
                try {
                    serverSocketChannel.close();
                } catch (IOException ignore) {
                }
            }
            if (selector != null) {
                try {
                    selector.close();
                } catch (IOException ignore) {
                }
            }
        }
    }

    /**
     * 初始化ServerSocketChannel，向Selector注册
     *
     * @throws IOException IO异常
     */
    private void initServerSocketChannel() throws IOException {
        try {
            selector = Selector.open();
            serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.configureBlocking(false);
            serverSocketChannel.socket().bind(new InetSocketAddress(NAME_NODE_IMAGE_UPLOAD_SERVER_PORT), 100);
            serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);

            logger.debug("name node image upload server start and listen {} port", NAME_NODE_IMAGE_UPLOAD_SERVER_PORT);
        } catch (IOException e) {
            logger.error("{} while init server socket channel", e.getMessage());
            throw e;
        }
    }

    /**
     * 处理上传镜像请求
     */
    private void process() throws IOException {
        while (!server.isClosing()) {
            selector.select(100);  // 阻塞 100ms ok?
            Iterator<SelectionKey> keysIterator = selector.selectedKeys().iterator();

            while (keysIterator.hasNext()) {
                SelectionKey key = keysIterator.next();
                keysIterator.remove();
                handleRequest(key);
            }
        }
    }

    /**
     * 处理请求
     *
     * @param key SelectionKey
     * @throws IOException IO异常
     */
    private void handleRequest(SelectionKey key) throws IOException {
        if (key.isAcceptable()) {
            handleConnectRequest(key);
        } else if (key.isReadable()) {
            handleReadableRequest(key);
        } else if (key.isWritable()) {
            handleWritableRequest(key);
        }
    }

    /**
     * 处理备份节点连接请求
     *
     * @param key SelectionKey
     * @throws IOException IO异常
     */
    private void handleConnectRequest(SelectionKey key) throws IOException {
        ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key.channel();
        SocketChannel channel = serverSocketChannel.accept();

        if (channel != null) {
            channel.configureBlocking(false);
            channel.register(selector, SelectionKey.OP_READ);   // 向Selector注册SocketChannel读请求
        }
    }

    /**
     * 处理备份节点上传镜像响应
     *
     * @param key SelectionKey
     * @throws IOException IO异常
     */
    private void handleWritableRequest(SelectionKey key) throws IOException {
        String response = "SUCCESS";
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        buffer.put(response.getBytes());
        buffer.flip();

        try (SocketChannel channel = (SocketChannel) key.channel()) {
            channel.write(buffer);
            logger.debug("receive fsImage finish from backup node, then return {} response", response);
        }
    }

    /**
     * 处理备份节点上传镜像请求
     *
     * @param key SelectionKey
     * @throws IOException IO异常
     */
    private void handleReadableRequest(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();

        if (!beforeWriteFsImage(channel)) {
            return;
        }

        writeFSImage();

        afterWriteFsImage();

        channel.register(selector, SelectionKey.OP_WRITE);  // 向Selector注册SocketChannel写请求
    }

    /**
     * 持久化镜像前置处理
     *
     * @param channel 客户端连接
     * @return true，接收到完整镜像请求且前置任务处理完成；否则，不可继续
     * @throws IOException IO异常
     */
    private boolean beforeWriteFsImage(SocketChannel channel) throws IOException {

        if (!doHandleReadableRequest(channel)) {
            return false;
        }

        Path checkPointFile = PathUtils.getFSImageFile(CHECKPOINT_PATH,
                fsNameSystem.getRuntimeRepository().getLongParameter(LAST_CHECKPOINT_MAX_TX_ID, 0),
                fsNameSystem.getRuntimeRepository().getLongParameter(LAST_CHECKPOINT_TIMESTAMP, 0),
                false);

        // 不存在上一次备份的镜像
        if (!Files.exists(checkPointFile)) {
            logger.debug("last checkpoint file {} not exists", checkPointFile.toFile().getPath());
            return true;
        }

        Path lastCheckPointFile = PathUtils.getFSImageFile(CHECKPOINT_PATH,
                fsNameSystem.getRuntimeRepository().getLongParameter(LAST_CHECKPOINT_MAX_TX_ID, 0),
                fsNameSystem.getRuntimeRepository().getLongParameter(LAST_CHECKPOINT_TIMESTAMP, 0),
                true);

        Files.move(checkPointFile, lastCheckPointFile); // 重命名上一次生成的镜像

        logger.debug("last checkpoint file {} rename to {}",
                checkPointFile.toFile().getPath(),
                lastCheckPointFile.toFile().getPath());

        return true;
    }

    /**
     * 处理备份节点上传镜像请求获取完整镜像报文
     *
     * @param channel 客户端连接
     * @return true，获取到完整镜像报文；否则，未获取到完整报文，需要继续读
     */
    private boolean doHandleReadableRequest(SocketChannel channel) throws IOException {
        if (!readCheckPointMaxTxId(channel)) {
            return false;
        }

        if (!readCheckPointTimestamp(channel)) {
            return false;
        }

        if (!readDirectoryLength(channel)) {
            return false;
        }

        return readDirectory(channel);
    }

    /**
     * 读取检查点的最大事务标识
     *
     * @param channel 客户端连接
     * @return 是否读取完成；true，读取完成；否则，未读取完
     * @throws IOException IO异常
     */
    private boolean readCheckPointMaxTxId(SocketChannel channel) throws IOException {
        if (cachedFSImage.checkPointMaxTxId != 0) {
            logger.debug("checkPointMaxTxId {} has decode", cachedFSImage.checkPointMaxTxId);
            return true; // 已解析
        }

        if (this.cachedCheckPointMaxTxId == null) {
            // 第一次读
            this.cachedCheckPointMaxTxId = ByteBuffer.allocate(8);
            logger.debug("first read checkPointMaxTxId, allocate 8 bytes");
        } else {
            // 拆包
            logger.debug("read again checkPointMaxTxId");
        }

        channel.read(this.cachedCheckPointMaxTxId);

        if (this.cachedCheckPointMaxTxId.hasRemaining()) {
            // 一次没有全部读完等待下一次读取，发生拆包
            logger.debug("not read all data for checkPointMaxTxId, need to read again");
            return false;
        }

        // 全部读完缓存
        this.cachedCheckPointMaxTxId.flip();
        this.cachedFSImage.checkPointMaxTxId = this.cachedCheckPointMaxTxId.getLong();
        logger.debug("decode finish, checkPointMaxTxId is {}", this.cachedFSImage.checkPointMaxTxId);
        return true;
    }

    /**
     * 读取检查点完成的时间戳
     *
     * @param channel 客户端连接
     * @return 是否读取完成；true，读取完成；否则，未读取完
     * @throws IOException IO异常
     */
    private boolean readCheckPointTimestamp(SocketChannel channel) throws IOException {
        if (cachedFSImage.checkPointTimestamp != 0) {
            logger.debug("checkPointTimestamp {} has decode", cachedFSImage.checkPointTimestamp);
            return true; // 已解析
        }

        if (this.cachedCheckPointTimestamp == null) {
            // 第一次读
            this.cachedCheckPointTimestamp = ByteBuffer.allocate(8);
            logger.debug("first read checkPointTimestamp, allocate 8 bytes");
        } else {
            // 拆包
            logger.debug("read again checkPointTimestamp");
        }

        channel.read(this.cachedCheckPointTimestamp);

        if (this.cachedCheckPointTimestamp.hasRemaining()) {
            // 一次没有全部读完等待下一次读取，发生拆包
            logger.debug("not read all data for checkPointTimestamp, need to read again");
            return false;
        }

        // 全部读完缓存
        this.cachedCheckPointTimestamp.flip();
        this.cachedFSImage.checkPointTimestamp = this.cachedCheckPointTimestamp.getLong();
        logger.debug("decode finish, checkPointTimestamp is {}", this.cachedFSImage.checkPointTimestamp);
        return true;
    }

    /**
     * 读取目录镜像长度
     *
     * @param channel 客户端连接
     * @return 是否读取完成；true，读取完成；否则，未读取完
     * @throws IOException IO异常
     */
    private boolean readDirectoryLength(SocketChannel channel) throws IOException {
        if (cachedFSImage.directoryLength != 0) {
            logger.debug("directoryLength {} has decode", cachedFSImage.directoryLength);
            return true; // 已解析
        }

        if (this.cachedDirectoryLength == null) {
            // 第一次读
            this.cachedDirectoryLength = ByteBuffer.allocate(4);
            logger.debug("first read directoryLength, allocate 4 bytes");
        } else {
            // 拆包
            logger.debug("read again directoryLength");
        }

        channel.read(this.cachedDirectoryLength);

        if (this.cachedDirectoryLength.hasRemaining()) {
            // 一次没有全部读完等待下一次读取，发生拆包
            logger.debug("not read all data for directoryLength, need to read again");
            return false;
        }

        // 全部读完缓存
        this.cachedDirectoryLength.flip();
        this.cachedFSImage.directoryLength = this.cachedDirectoryLength.getInt();
        logger.debug("decode finish, directoryLength is {}", this.cachedFSImage.directoryLength);
        return true;
    }

    /**
     * 读取目录镜像
     *
     * @param channel 客户端连接
     * @return 是否读取完成；true，读取完成；否则，未读取完
     * @throws IOException IO异常
     */
    private boolean readDirectory(SocketChannel channel) throws IOException {
        if (this.cachedDirectory == null) {
            // 第一次读
            this.cachedDirectory = ByteBuffer.allocate(cachedFSImage.directoryLength);
            logger.debug("first read directory, allocate {} bytes", cachedFSImage.directoryLength);
        } else {
            // 拆包
            logger.debug("read again directory");
        }

        channel.read(this.cachedDirectory);

        if (this.cachedDirectory.hasRemaining()) {
            // 一次没有全部读完等待下一次读取，发生拆包
            logger.debug("not read all data for directory, need to read again");
            return false;
        }

        // 全部读完缓存
        this.cachedDirectory.flip();
        this.cachedFSImage.directory = this.cachedDirectory;
        logger.debug("decode finish, directory size is {} bytes", this.cachedFSImage.directory.capacity());
        return true;
    }

    private void writeFSImage() throws IOException {
        FileChannel fileChannel = null;

        try {
            Path checkPointFile = PathUtils.getFSImageFile(CHECKPOINT_PATH,
                    cachedFSImage.checkPointMaxTxId, cachedFSImage.checkPointTimestamp, false);

            fileChannel = FileChannel.open(checkPointFile, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            fileChannel.write(cachedFSImage.directory);
            fileChannel.force(false);   // 强制刷写到磁盘

            logger.debug("max txId is {}, timestamp is {} finish to write fsImage file {}",
                    cachedFSImage.checkPointMaxTxId, cachedFSImage.checkPointTimestamp, checkPointFile.toFile().getPath());

        } catch (IOException e) {
            logger.error("{} while write fsImage", e.getMessage());
            throw e;
        } finally {
            if (fileChannel != null) {
                try {
                    fileChannel.close();
                } catch (IOException ignore) {
                }
            }
        }
    }

    /**
     * 持久化镜像后置处理
     */
    private void afterWriteFsImage() throws IOException {
        // 若存在临时镜像文件，则删除
        Path lastCheckPointFile = PathUtils.getFSImageFile(CHECKPOINT_PATH,
                fsNameSystem.getRuntimeRepository().getLongParameter(LAST_CHECKPOINT_MAX_TX_ID, 0),
                fsNameSystem.getRuntimeRepository().getLongParameter(LAST_CHECKPOINT_TIMESTAMP, 0),
                true);
        if (Files.deleteIfExists(lastCheckPointFile)) {
            logger.debug("delete last checkpoint file {}", lastCheckPointFile.toFile().getPath());
        }

        // 更新本地checkpoint信息
        fsNameSystem.getRuntimeRepository().setParameter(LAST_CHECKPOINT_MAX_TX_ID, cachedFSImage.checkPointMaxTxId);
        fsNameSystem.getRuntimeRepository().setParameter(LAST_CHECKPOINT_TIMESTAMP, cachedFSImage.checkPointTimestamp);

        // 清空缓存
        this.cachedCheckPointMaxTxId.clear();
        this.cachedCheckPointMaxTxId = null;

        this.cachedCheckPointTimestamp.clear();
        this.cachedCheckPointTimestamp = null;

        this.cachedDirectoryLength.clear();
        this.cachedDirectoryLength = null;

        this.cachedDirectory.clear();
        this.cachedDirectory = null;

        this.cachedFSImage.clear();
    }
}
