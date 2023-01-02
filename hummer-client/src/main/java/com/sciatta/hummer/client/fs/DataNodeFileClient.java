package com.sciatta.hummer.client.fs;

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
     * 发送文件
     *
     * @param hostname 主机名
     * @param port     端口
     * @param file     文件字节数组
     * @param fileName 文件名
     * @param fileSize 文件大小
     */
    public static void sendFile(String hostname, int port, byte[] file, String fileName, long fileSize) {
        SocketChannel channel = null;
        Selector selector = null;

        try {
            selector = Selector.open();

            channel = SocketChannel.open();
            channel.configureBlocking(false);
            channel.connect(new InetSocketAddress(hostname, port));

            channel.register(selector, SelectionKey.OP_CONNECT);

            boolean sending = true;

            while (sending) {
                selector.select();

                Iterator<SelectionKey> keysIterator = selector.selectedKeys().iterator();
                while (keysIterator.hasNext()) {
                    SelectionKey key = keysIterator.next();
                    keysIterator.remove();

                    if (key.isConnectable()) {
                        channel = (SocketChannel) key.channel();

                        if (channel.isConnectionPending()) {
                            channel.finishConnect();

                            // 封装文件的请求数据
                            byte[] filenameBytes = fileName.getBytes();

                            ByteBuffer buffer = ByteBuffer.allocate(4 + filenameBytes.length + 8 + file.length);

                            buffer.putInt(filenameBytes.length); // 文件名字节数
                            buffer.put(filenameBytes); // 文件名
                            buffer.putLong(fileSize); // 文件字节数
                            buffer.put(file);   // 文件

                            buffer.flip();

                            int writeSize = channel.write(buffer);
                            logger.debug("send {} bytes to {}:{}", writeSize, hostname, port);

                            channel.register(selector, SelectionKey.OP_READ);
                        }

                    } else if (key.isReadable()) {
                        channel = (SocketChannel) key.channel();

                        ByteBuffer buffer = ByteBuffer.allocate(1024);
                        int len = channel.read(buffer);

                        if (len > 0) {
                            logger.debug("receive {} response from {}:{}", new String(buffer.array(), 0, len), hostname, port);
                            sending = false;
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.error("{} while send file {} to {}:{}", e.getMessage(), fileName, hostname, port);
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
}
