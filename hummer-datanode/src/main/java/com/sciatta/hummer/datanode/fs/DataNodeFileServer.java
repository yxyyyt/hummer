package com.sciatta.hummer.datanode.fs;

import com.sciatta.hummer.client.rpc.NameNodeRpcClient;
import com.sciatta.hummer.core.exception.HummerException;
import com.sciatta.hummer.core.server.Server;
import com.sciatta.hummer.core.transport.RequestType;
import com.sciatta.hummer.core.util.PathUtils;
import com.sciatta.hummer.datanode.config.DataNodeConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by Rain on 2023/1/2<br>
 * All Rights Reserved(C) 2017 - 2023 SCIATTA <br> <p/>
 * 数据节点文件服务
 */
public class DataNodeFileServer extends Thread {
    private static final Logger logger = LoggerFactory.getLogger(DataNodeFileServer.class);

    /**
     * 任务处理队列
     */
    private final List<LinkedBlockingQueue<SelectionKey>> queues = new ArrayList<>();

    /**
     * 缓存没读取完的文件数据
     */
    private final Map<String, CachedRequest> cachedRequests = new ConcurrentHashMap<>();

    /**
     * 缓存没读取完的请求类型
     */
    private final Map<String, ByteBuffer> requestTypeByClient = new ConcurrentHashMap<>();

    /**
     * 缓存没读取完的文件名长度
     */
    private final Map<String, ByteBuffer> fileNameLengthByClient = new ConcurrentHashMap<>();

    /**
     * 缓存没读取完的文件名
     */
    private final Map<String, ByteBuffer> fileNameByClient = new ConcurrentHashMap<>();

    /**
     * 缓存没读取完的文件长度
     */
    private final Map<String, ByteBuffer> fileLengthByClient = new ConcurrentHashMap<>();

    /**
     * 缓存没读取完的文件
     */
    private final Map<String, ByteBuffer> fileByClient = new ConcurrentHashMap<>();

    private final NameNodeRpcClient nameNodeRpcClient;
    private final Server server;

    private Selector selector;

    public DataNodeFileServer(Server server, NameNodeRpcClient nameNodeRpcClient) {
        this.server = server;
        this.nameNodeRpcClient = nameNodeRpcClient;

        // 初始化服务通道
        initServerSocketChannel();
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

    @Override
    public void run() {
        while (!server.isClosing()) {
            try {
                selector.select();
                Iterator<SelectionKey> keysIterator = selector.selectedKeys().iterator();

                while (keysIterator.hasNext()) {
                    SelectionKey key = keysIterator.next();
                    keysIterator.remove();
                    handleSelectedKey(key);
                }
            } catch (Throwable e) {
                logger.error("{} while handle selected key", e.getMessage());
            }
        }
    }

    /**
     * 处理SelectionKey
     *
     * @param key SelectionKey
     * @throws IOException          IO异常
     * @throws InterruptedException 中断异常
     */
    private void handleSelectedKey(SelectionKey key) throws IOException, InterruptedException {
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

            // 选择工作队列，设置SelectionKey
            int queueIndex = remoteAddress.hashCode() % queues.size();
            queues.get(queueIndex).put(key);

            // 取消读事件，防止空轮训
            key.interestOps(key.interestOps() & ~SelectionKey.OP_READ);
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

                    if (!handleRequest(channel, key)) {
                        // 没有读取完成，重新注册读请求
                        key.interestOps(key.interestOps() & SelectionKey.OP_READ);
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
     * 处理客户端请求
     *
     * @param channel 客户端通道
     * @param key     SelectionKey
     * @return 是否读取完成；true，读取完成；否则，没有读取完成
     * @throws IOException IO异常
     */
    private boolean handleRequest(SocketChannel channel, SelectionKey key) throws IOException {
        String client = channel.getRemoteAddress().toString();

        logger.debug("start to handle {} request", client);

        // 请求类型
        int requestType;
        if ((requestType = getRequestType(channel)) == 0) {
            return false;
        } else {
            logger.debug("get request type {} from request", requestType);
        }

        boolean ans = false;

        if (RequestType.UPLOAD_FILE == requestType) {
            ans = handleUploadFileRequest(channel, key);
        } else if (RequestType.DOWNLOAD_FILE == requestType) {
            ans = handleDownloadFileRequest(channel, key);
        }

        return ans;
    }

    /**
     * 获取请求类型
     *
     * @param channel 客户端通道
     * @return 请求类型
     * @throws IOException IO异常
     */
    private int getRequestType(SocketChannel channel) throws IOException {
        int requestType = 0;
        String client = channel.getRemoteAddress().toString();

        if (getCachedRequest(client).requestType != 0) {
            return getCachedRequest(client).requestType;
        }

        ByteBuffer requestTypeBuffer;
        if (requestTypeByClient.containsKey(client)) {
            requestTypeBuffer = requestTypeByClient.get(client);
        } else {
            requestTypeBuffer = ByteBuffer.allocate(4);
        }

        channel.read(requestTypeBuffer);

        if (!requestTypeBuffer.hasRemaining()) {
            requestTypeBuffer.rewind();
            requestType = requestTypeBuffer.getInt();
            getCachedRequest(client).requestType = requestType;

            requestTypeByClient.remove(client);
        } else {
            requestTypeByClient.put(client, requestTypeBuffer);
        }

        return requestType;
    }

    /**
     * 获取缓存的请求
     *
     * @param client 客户端唯一标识
     * @return 缓存的请求
     */
    private CachedRequest getCachedRequest(String client) {
        CachedRequest cachedRequest = cachedRequests.get(client);
        if (cachedRequest == null) {
            cachedRequest = cachedRequests.getOrDefault(client, new CachedRequest());
        }
        return cachedRequest;
    }

    /**
     * 处理上传文件请求
     *
     * @param channel 客户端通道
     * @param key     SelectionKey
     * @return 是否读取完成；true，读取完成；否则，没有读取完成
     * @throws IOException IO异常
     */
    private boolean handleUploadFileRequest(SocketChannel channel, SelectionKey key) throws IOException {
        String client = channel.getRemoteAddress().toString();

        // 文件名
        FileName fileName = getFileName(channel);
        logger.debug("get file name {} from request", fileName);
        if (fileName == null) {
            return false;
        }

        // 文件长度
        long fileLength = getFileLength(channel);
        logger.debug("get file length {} from request", fileLength);
        if (fileLength == 0) {
            return false;
        }

        // 文件
        FileOutputStream fileOutputStream = null;
        FileChannel fileChannel = null;
        try {
            fileOutputStream = new FileOutputStream(fileName.absoluteFileName);
            fileChannel = fileOutputStream.getChannel();
            fileChannel.position(fileChannel.size());   // 定位到不完整文件的末端

            ByteBuffer fileBuffer;
            if (fileByClient.containsKey(client)) {
                fileBuffer = fileByClient.get(client);
            } else {
                fileBuffer = ByteBuffer.allocate((int) fileLength);
            }

            long hasReadFileSize = getHasReadFileLength(channel);
            hasReadFileSize += channel.read(fileBuffer);

            if (!fileBuffer.hasRemaining()) {
                fileBuffer.rewind();
                fileChannel.write(fileBuffer);
                logger.debug("write {}({}) bytes to {}", hasReadFileSize, fileLength, fileName.absoluteFileName);

                fileByClient.remove(client);

                String response = "SUCCESS";
                ByteBuffer outBuffer = ByteBuffer.wrap(response.getBytes());
                channel.write(outBuffer);
                logger.debug("file has been read completely, send {} to {}", response, client);

                cachedRequests.remove(client);

                // 向元数据节点增量上报文件
                nameNodeRpcClient.incrementalReport(
                        DataNodeConfig.getLocalHostname(),
                        DataNodeConfig.getLocalPort(),
                        fileName.relativeFileName,
                        fileLength);
                logger.debug("incremental report file name {} success", fileName.relativeFileName);

                return true;
            } else {
                fileByClient.put(client, fileBuffer);
                getCachedRequest(client).hasReadFileLength = hasReadFileSize;
                logger.debug("write {}({}) bytes to {}", hasReadFileSize, fileLength, fileName.absoluteFileName);

                return false;
            }
        } finally {
            if (fileChannel != null) fileChannel.close();
            if (fileOutputStream != null) fileOutputStream.close();
        }
    }

    /**
     * 获取文件名
     *
     * @param channel 客户端通道
     * @return 文件名
     * @throws IOException IO异常
     */
    private FileName getFileName(SocketChannel channel) throws IOException {
        FileName fileName;
        String client = channel.getRemoteAddress().toString();

        if (getCachedRequest(client).fileName != null) {
            return getCachedRequest(client).fileName;
        } else {
            String relativeFileName = getRelativeFileName(channel);
            if (relativeFileName == null) {
                return null;
            }

            fileName = new FileName();
            fileName.relativeFileName = relativeFileName;
            fileName.absoluteFileName = PathUtils.getAbsoluteFileName(DataNodeConfig.getDataNodeDataPath(),
                    relativeFileName);

            getCachedRequest(client).fileName = fileName;
        }

        return fileName;
    }

    /**
     * 获取相对路径的文件名
     *
     * @param channel 客户端通道
     * @return 相对路径的文件名
     */
    private String getRelativeFileName(SocketChannel channel) throws IOException {
        String client = channel.getRemoteAddress().toString();

        int fileNameLength = 0;
        String fileName = null;

        // 文件名长度
        if (!fileNameByClient.containsKey(client)) {
            ByteBuffer fileNameLengthBuffer;
            if (fileNameLengthByClient.containsKey(client)) {
                fileNameLengthBuffer = fileNameLengthByClient.get(client);
            } else {
                fileNameLengthBuffer = ByteBuffer.allocate(4);
            }

            channel.read(fileNameLengthBuffer);

            if (!fileNameLengthBuffer.hasRemaining()) {
                fileNameLengthBuffer.rewind();
                fileNameLength = fileNameLengthBuffer.getInt();

                fileNameLengthByClient.remove(client);
            } else {
                fileNameLengthByClient.put(client, fileNameLengthBuffer);
                return null;
            }
        }

        // 文件名
        ByteBuffer fileNameBuffer;
        if (fileNameByClient.containsKey(client)) {
            fileNameBuffer = fileNameByClient.get(client);
        } else {
            fileNameBuffer = ByteBuffer.allocate(fileNameLength);
        }

        channel.read(fileNameBuffer);

        if (!fileNameBuffer.hasRemaining()) {
            fileNameBuffer.rewind();
            fileName = new String(fileNameBuffer.array());

            fileNameByClient.remove(client);
        } else {
            fileNameByClient.put(client, fileNameBuffer);
        }

        return fileName;
    }

    /**
     * 获取文件长度
     *
     * @param channel 客户端通道
     * @return 文件长度
     * @throws IOException IO异常
     */
    private long getFileLength(SocketChannel channel) throws IOException {
        long fileLength = 0;
        String client = channel.getRemoteAddress().toString();

        if (getCachedRequest(client).fileLength != 0) {
            return getCachedRequest(client).fileLength;
        } else {
            ByteBuffer fileLengthBuffer;
            if (fileLengthByClient.get(client) != null) {
                fileLengthBuffer = fileLengthByClient.get(client);
            } else {
                fileLengthBuffer = ByteBuffer.allocate(8);
            }

            channel.read(fileLengthBuffer);

            if (!fileLengthBuffer.hasRemaining()) {
                fileLengthBuffer.rewind();
                fileLength = fileLengthBuffer.getLong();
                getCachedRequest(client).fileLength = fileLength;

                fileLengthByClient.remove(client);
            } else {
                fileLengthByClient.put(client, fileLengthBuffer);
            }
        }

        return fileLength;
    }

    /**
     * 获取已经读取的文件长度
     *
     * @param channel 客户端通道
     * @return 已经读取的文件长度
     * @throws IOException IO异常
     */
    private long getHasReadFileLength(SocketChannel channel) throws IOException {
        long hasReadFileLength = 0;
        String remoteAddress = channel.getRemoteAddress().toString();
        if (cachedRequests.containsKey(remoteAddress)) {
            hasReadFileLength = cachedRequests.get(remoteAddress).hasReadFileLength;
        }
        return hasReadFileLength;
    }

    /**
     * 处理下载文件请求
     *
     * @param channel 客户端通道
     * @param key     SelectionKey
     * @return 是否读取完成；true，读取完成；否则，没有读取完成
     * @throws IOException IO异常
     */
    private boolean handleDownloadFileRequest(SocketChannel channel, SelectionKey key) throws IOException {
        String client = channel.getRemoteAddress().toString();

        FileName fileName = getFileName(channel);
        logger.debug("get file name {} from request", fileName);
        if (fileName == null) {
            return false;
        }

        File file = new File(fileName.absoluteFileName);
        long fileLength = file.length();

        FileInputStream fileInputStream = new FileInputStream(fileName.absoluteFileName);
        FileChannel fileChannel = fileInputStream.getChannel();

        int hasReadFileLength = 0;
        try {
            ByteBuffer buffer = ByteBuffer.allocate(8 + (int) fileLength);

            // 文件字节数
            buffer.putLong(fileLength);

            // 文件
            while (buffer.hasRemaining()) {
                hasReadFileLength += fileChannel.read(buffer);
                logger.debug("read {}({}) bytes from {}", hasReadFileLength, fileLength, fileName.absoluteFileName);
            }

            buffer.rewind();
            int write = channel.write(buffer);
            logger.debug("write file {}({}) bytes to {} finish", write, buffer.capacity(), client);

            cachedRequests.remove(client);

            return true;
        } finally {
            fileInputStream.close();
            fileChannel.close();
        }
    }

    /**
     * 缓存请求
     */
    private static class CachedRequest {
        /**
         * 请求类型
         */
        int requestType;

        /**
         * 文件名
         */
        FileName fileName;

        /**
         * 文件长度
         */
        long fileLength;

        /**
         * 已经读取文件长度
         */
        long hasReadFileLength;

        @Override
        public String toString() {
            return "CachedRequest{" +
                    "requestType=" + requestType +
                    ", fileName=" + fileName +
                    ", fileLength=" + fileLength +
                    ", hasReadFileLength=" + hasReadFileLength +
                    '}';
        }
    }

    /**
     * 文件名
     */
    private static class FileName {

        /**
         * 相对路径
         */
        private String relativeFileName;

        /**
         * 绝对路径
         */
        private String absoluteFileName;

        @Override
        public String toString() {
            return "FileName{" +
                    "relativeFileName='" + relativeFileName + '\'' +
                    ", absoluteFileName='" + absoluteFileName + '\'' +
                    '}';
        }
    }
}
