package com.sciatta.hummer.client.fs;

import com.sciatta.hummer.core.transport.RequestType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

/**
 * Created by Rain on 2023/1/2<br>
 * All Rights Reserved(C) 2017 - 2023 SCIATTA <br> <p/>
 * 数据节点通信客户端
 */
public class DataNodeFileClient {
    private static final Logger logger = LoggerFactory.getLogger(DataNodeFileClient.class);

    /**
     * 上传文件
     *
     * @param hostname 主机名
     * @param port     端口
     * @param file     文件字节数组
     * @param fileName 文件名
     * @param fileSize 文件大小
     */
    public void uploadFile(String hostname, int port, byte[] file, String fileName, long fileSize) {
        SocketChannel channel = null;
        Selector selector = null;
        ByteBuffer buffer = null;

        try {
            selector = Selector.open();

            channel = SocketChannel.open();
            channel.configureBlocking(false);
            channel.connect(new InetSocketAddress(hostname, port));

            channel.register(selector, SelectionKey.OP_CONNECT);

            boolean sending = true;
            int hasWriteLength = 0; // 已经输出的长度

            while (sending) {
                selector.select();

                Iterator<SelectionKey> keysIterator = selector.selectedKeys().iterator();
                while (keysIterator.hasNext()) {
                    SelectionKey key = keysIterator.next();
                    keysIterator.remove();

                    if (key.isConnectable()) {
                        channel = (SocketChannel) key.channel();

                        if (channel.isConnectionPending()) {
                            // 建立连接
                            while (!channel.finishConnect()) {
                                Thread.sleep(10);
                            }
                        }

                        byte[] filenameBytes = fileName.getBytes();

                        buffer = ByteBuffer.allocate(4 + 4 + filenameBytes.length + 8 + file.length);

                        buffer.putInt(RequestType.UPLOAD_FILE); // 请求类型
                        buffer.putInt(filenameBytes.length); // 文件名字节数
                        buffer.put(filenameBytes); // 文件名
                        buffer.putLong(fileSize); // 文件字节数
                        buffer.put(file);   // 文件字节数组

                        buffer.flip();

                        hasWriteLength += channel.write(buffer);
                        logger.debug("write {}/{} bytes to {}:{}", hasWriteLength, buffer.capacity(), hostname, port);

                        if (buffer.hasRemaining()) {
                            // 发送时发生拆包，需要多次发送
                            key.interestOps(SelectionKey.OP_WRITE);
                        } else {
                            // 发送完成
                            key.interestOps(SelectionKey.OP_READ);
                        }
                    } else if (key.isWritable()) {
                        channel = (SocketChannel) key.channel();

                        hasWriteLength += channel.write(buffer);
                        logger.debug("write {}/{} bytes to {}:{}", hasWriteLength, buffer.capacity(), hostname, port);

                        if (buffer.hasRemaining()) {
                            // 发送时发生拆包，需要多次发送
                            key.interestOps(SelectionKey.OP_WRITE);
                        } else {
                            // 发送完成
                            key.interestOps(SelectionKey.OP_READ);
                        }
                    } else if (key.isReadable()) {
                        channel = (SocketChannel) key.channel();

                        buffer = ByteBuffer.allocate(1024);
                        int len = channel.read(buffer);

                        if (len > 0) {
                            logger.debug("read {} from {}:{}", new String(buffer.array(), 0, len), hostname, port);
                            sending = false;
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.error("{} while upload file {} to {}:{}", e.getMessage(), fileName, hostname, port);
        } finally {
            if (channel != null) {
                try {
                    channel.close();
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
     * 下载文件
     *
     * @param hostname 主机名
     * @param port     端口
     * @param fileName 文件名
     * @return 文件字节数组；若不存在，返回null
     */
    public byte[] downloadFile(String hostname, int port, String fileName) {
        ByteBuffer fileLengthBuffer = null;
        long fileLength = 0;
        ByteBuffer fileBuffer = null;

        byte[] file = null;

        SocketChannel channel = null;
        Selector selector = null;

        try {
            channel = SocketChannel.open();
            channel.configureBlocking(false);
            channel.connect(new InetSocketAddress(hostname, port));

            selector = Selector.open();
            channel.register(selector, SelectionKey.OP_CONNECT);

            boolean reading = true;
            int hasReadLength = 0; // 已经读取的长度

            while (reading) {
                selector.select();

                Iterator<SelectionKey> keysIterator = selector.selectedKeys().iterator();
                while (keysIterator.hasNext()) {
                    SelectionKey key = keysIterator.next();
                    keysIterator.remove();

                    if (key.isConnectable()) {
                        channel = (SocketChannel) key.channel();

                        if (channel.isConnectionPending()) {
                            // 建立连接
                            while (!channel.finishConnect()) {
                                Thread.sleep(10);
                            }
                        }

                        byte[] fileNameBytes = fileName.getBytes();

                        ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + fileNameBytes.length);

                        buffer.putInt(RequestType.DOWNLOAD_FILE);  // 请求类型
                        buffer.putInt(fileNameBytes.length);   // 文件名字节数组
                        buffer.put(fileNameBytes); // 文件名

                        buffer.flip();

                        int write = channel.write(buffer);

                        logger.debug("write {}/{} bytes to {}:{}", write, buffer.capacity(), hostname, port);

                        key.interestOps(SelectionKey.OP_READ);
                    } else if (key.isReadable()) {
                        channel = (SocketChannel) key.channel();

                        // 文件字节数
                        if (fileLength == 0) {
                            if (fileLengthBuffer == null) {
                                fileLengthBuffer = ByteBuffer.allocate(8);
                            }

                            channel.read(fileLengthBuffer);

                            if (!fileLengthBuffer.hasRemaining()) { // 读完
                                fileLengthBuffer.rewind();
                                fileLength = fileLengthBuffer.getLong();
                                logger.debug("read file length {}", fileLength);
                            }
                        }

                        // 文件
                        if (fileLength != 0) {
                            if (fileBuffer == null) {
                                fileBuffer = ByteBuffer.allocate((int) fileLength);
                            }
                            hasReadLength += channel.read(fileBuffer);
                            logger.debug("read file {}/{} bytes from {}:{}", hasReadLength, fileBuffer.capacity(),
                                    hostname, port);

                            if (!fileBuffer.hasRemaining()) {   // 读完
                                fileBuffer.rewind();
                                file = fileBuffer.array();
                                logger.debug("read file {} bytes from {}:{} finish", file.length, hostname, port);
                                reading = false;
                            }
                        }
                    }
                }
            }

            return file;
        } catch (Exception e) {
            logger.error("{} while download file {} from {}:{}", e.getMessage(), fileName, hostname, port);
        } finally {
            if (channel != null) {
                try {
                    channel.close();
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

        return null;
    }
}
