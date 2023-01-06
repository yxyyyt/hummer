package com.sciatta.hummer.datanode.server.fs;

import com.sciatta.hummer.core.exception.HummerException;
import com.sciatta.hummer.core.server.Server;
import com.sciatta.hummer.core.util.PathUtils;
import com.sciatta.hummer.datanode.server.config.DataNodeConfig;
import com.sciatta.hummer.datanode.server.rpc.NameNodeRpcClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by Rain on 2023/1/2<br>
 * All Rights Reserved(C) 2017 - 2023 SCIATTA <br> <p/>
 * 数据节点文件服务
 */
public class DataNodeFileServer extends Thread {
    private static final Logger logger = LoggerFactory.getLogger(DataNodeFileServer.class);

    private Selector selector;

    /**
     * 任务处理队列
     */
    private final List<LinkedBlockingQueue<SelectionKey>> queues = new ArrayList<>();

    /**
     * 缓存文件
     */
    private final Map<String, CachedFile> cachedFileMap = new HashMap<>();

    private final NameNodeRpcClient nameNodeRpcClient;

    private final Server server;

    public DataNodeFileServer(Server server, NameNodeRpcClient nameNodeRpcClient) {
        this.server = server;
        this.nameNodeRpcClient = nameNodeRpcClient;

        // 初始化服务通道
        initServerSocketChannel();
    }

    @Override
    public void run() {
        while (!server.isClosing()) {
            try {
                selector.select();
                Iterator<SelectionKey> keysIterator = selector.selectedKeys().iterator();

                while (keysIterator.hasNext()) {
                    SelectionKey key = keysIterator.next();
                    keysIterator.remove();
                    handleRequest(key);
                }
            } catch (Throwable e) {
                logger.error("{} while handle request", e.getMessage());
            }
        }
    }

    /**
     * 初始化服务通道
     */
    private void initServerSocketChannel() {
        ServerSocketChannel serverSocketChannel;

        try {
            selector = Selector.open();

            serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.configureBlocking(false);
            serverSocketChannel.socket().bind(new InetSocketAddress(DataNodeConfig.getLocalPort()), 100);

            serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);

            for (int i = 0; i < DataNodeConfig.getFileServerWorkerCount(); i++) {
                LinkedBlockingQueue<SelectionKey> queue = new LinkedBlockingQueue<>();
                queues.add(queue);
                new Worker(queue).start();  // 启动工作线程
            }

            logger.debug("data node file server start and listen {} port", DataNodeConfig.getLocalPort());
        } catch (IOException e) {
            logger.error("{} while start data node file server", e.getMessage());
            throw new HummerException("%s while start data node file server", e.getMessage());
        }
    }

    /**
     * 处理请求
     *
     * @param key SelectionKey
     * @throws IOException          IO异常
     * @throws InterruptedException 中断异常
     */
    private void handleRequest(SelectionKey key) throws IOException, InterruptedException {
        SocketChannel channel;

        if (key.isAcceptable()) {
            ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key.channel();
            channel = serverSocketChannel.accept();
            if (channel != null) {
                channel.configureBlocking(false);
                channel.register(selector, SelectionKey.OP_READ);
            }
        } else if (key.isReadable()) {
            channel = (SocketChannel) key.channel();
            String remoteAddress = channel.getRemoteAddress().toString();

            // 选择工作队列
            int queueIndex = remoteAddress.hashCode() % queues.size();
            queues.get(queueIndex).put(key);
        }
    }

    /**
     * 工作线程
     */
    private class Worker extends Thread {

        private final LinkedBlockingQueue<SelectionKey> queue;

        public Worker(LinkedBlockingQueue<SelectionKey> queue) {
            this.queue = queue;
        }

        @Override
        public void run() {
            while (!server.isClosing()) {
                SocketChannel channel = null;

                try {
                    SelectionKey key = queue.take();

                    channel = (SocketChannel) key.channel();
                    if (!channel.isOpen()) {
                        channel.close();
                        continue;
                    }
                    String remoteAddress = channel.getRemoteAddress().toString();
                    logger.debug("{} request", remoteAddress);

                    ByteBuffer buffer = ByteBuffer.allocate(10 * 1024);

                    // 解析文件名
                    String fileName = getFileName(channel, buffer);
                    logger.debug("get file name {} from request", fileName);
                    if (fileName == null) {
                        channel.close();
                        continue;
                    }

                    // 解析文件大小
                    long fileSize = getFileSize(channel, buffer);
                    logger.debug("get file size {} from request", fileSize);

                    // 已经读取的文件大小
                    long hasReadFileSize = getHasReadFileSize(channel);

                    FileOutputStream fileOutputStream = new FileOutputStream(getFileOutPath(fileName));
                    FileChannel fileChannel = fileOutputStream.getChannel();
                    fileChannel.position(fileChannel.size());   // 可能会多次输出，定位到不完整文件的末端

                    // 第一次接收到请求，数据写入到本地文件
                    if (!cachedFileMap.containsKey(remoteAddress)) {
                        hasReadFileSize += fileChannel.write(buffer);
                        logger.debug("write {}/{} bytes to {}", hasReadFileSize, fileSize, getFileOutPath(fileName));
                        buffer.clear();
                    }

                    // 循环不断的从channel里读取数据，写入本地文件
                    int len;
                    while ((len = channel.read(buffer)) > 0) {
                        hasReadFileSize += len;
                        buffer.flip();
                        fileChannel.write(buffer);
                        buffer.clear();
                        logger.debug("write {}/{} bytes to {}", hasReadFileSize, fileSize, getFileOutPath(fileName));
                    }
                    fileChannel.close();
                    fileOutputStream.close();

                    if (hasReadFileSize == fileSize) {
                        // 读取完成，返回结果
                        String response = "SUCCESS";
                        ByteBuffer outBuffer = ByteBuffer.wrap(response.getBytes());
                        channel.write(outBuffer);
                        cachedFileMap.remove(remoteAddress);
                        logger.debug("file has been read completely, send {} response {}", remoteAddress, response);

                        // 向元数据节点增量上报文件
                        nameNodeRpcClient.incrementalReport(fileName);
                    } else {
                        // 未读取完成，缓存文件等待下一次读取
                        CachedFile cachedFile = new CachedFile(fileName, fileSize, hasReadFileSize);
                        cachedFileMap.put(remoteAddress, cachedFile);
                        key.interestOps(SelectionKey.OP_READ);
                        logger.debug("file has not been read completely, wait for read again {}", cachedFile);
                    }
                } catch (IOException | InterruptedException e) {
                    logger.error("{} while resolution request", e.getMessage());
                    if (channel != null) {
                        try {
                            channel.close();
                        } catch (IOException ignore) {
                        }
                    }
                }
            }
        }
    }

    /**
     * 获取文件名
     *
     * @param channel 客户端通道
     * @param buffer  字节缓存
     * @return 文件名
     * @throws IOException IO异常
     */
    private String getFileName(SocketChannel channel, ByteBuffer buffer) throws IOException {
        String fileName;
        String remoteAddress = channel.getRemoteAddress().toString();

        if (cachedFileMap.containsKey(remoteAddress)) {
            fileName = cachedFileMap.get(remoteAddress).fileName;
        } else {
            fileName = getFileNameFromChannel(channel, buffer);
        }

        return fileName;
    }

    /**
     * 获取文件输出路径
     *
     * @param fileName 文件名
     * @return 文件输出路径
     * @throws IOException IO异常
     */
    private String getFileOutPath(String fileName) throws IOException {
        String[] fileNameSplit = fileName.split(PathUtils.getINodeSeparator());

        StringBuilder filePath = new StringBuilder(DataNodeConfig.getDataNodeDataPath());
        for (String s : fileNameSplit) {
            if (s.equals("")) {
                continue;
            }
            filePath.append(PathUtils.getFileSeparator()).append(s);
        }

        PathUtils.getPathAndCreateDirectoryIfNotExists(filePath.toString());
        return filePath.toString();
    }

    /**
     * 从客户端请求中获取文件名
     *
     * @param channel 客户端通道
     * @param buffer  字节缓存
     * @return 文件名
     */
    private String getFileNameFromChannel(SocketChannel channel, ByteBuffer buffer) throws IOException {
        int len = channel.read(buffer);

        if (len > 0) {
            buffer.flip();

            byte[] fileNameLengthBytes = new byte[4];
            buffer.get(fileNameLengthBytes, 0, 4);

            ByteBuffer fileNameLengthBuffer = ByteBuffer.allocate(4);
            fileNameLengthBuffer.put(fileNameLengthBytes);
            fileNameLengthBuffer.flip();
            int fileNameLength = fileNameLengthBuffer.getInt();

            byte[] fileNameBytes = new byte[fileNameLength];
            buffer.get(fileNameBytes, 0, fileNameLength);

            return new String(fileNameBytes);
        }

        return null;
    }

    /**
     * 获取文件大小
     *
     * @param channel 客户端通道
     * @param buffer  字节缓存
     * @return 文件大小
     * @throws IOException IO异常
     */
    private long getFileSize(SocketChannel channel, ByteBuffer buffer) throws IOException {
        long fileSize;
        String remoteAddress = channel.getRemoteAddress().toString();

        if (cachedFileMap.containsKey(remoteAddress)) {
            fileSize = cachedFileMap.get(remoteAddress).fileSize;
        } else {
            byte[] fileSizeBytes = new byte[8];
            buffer.get(fileSizeBytes, 0, 8);

            ByteBuffer fileSizeBuffer = ByteBuffer.allocate(8);
            fileSizeBuffer.put(fileSizeBytes);
            fileSizeBuffer.flip();
            fileSize = fileSizeBuffer.getLong();
        }

        return fileSize;
    }

    /**
     * 获取已经读取的文件大小
     *
     * @param channel 客户端通道
     * @return 已经读取的文件大小
     * @throws IOException IO异常
     */
    private long getHasReadFileSize(SocketChannel channel) throws IOException {
        long hasReadImageLength = 0;
        String remoteAddress = channel.getRemoteAddress().toString();
        if (cachedFileMap.containsKey(remoteAddress)) {
            hasReadImageLength = cachedFileMap.get(remoteAddress).hasReadFileSize;
        }
        return hasReadImageLength;
    }

    /**
     * 缓存文件模型
     */
    private static class CachedFile {

        private final String fileName;
        private final long fileSize;
        private final long hasReadFileSize;

        public CachedFile(String fileName, long fileSize, long hasReadFileSize) {
            this.fileName = fileName;
            this.fileSize = fileSize;
            this.hasReadFileSize = hasReadFileSize;
        }

        @Override
        public String toString() {
            return "CachedFile{" +
                    "fileName='" + fileName + '\'' +
                    ", fileSize=" + fileSize +
                    ", hasReadFileSize=" + hasReadFileSize +
                    '}';
        }
    }
}
