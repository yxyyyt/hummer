package com.sciatta.hummer.backupnode.fs;

import com.sciatta.hummer.backupnode.config.BackupNodeConfig;
import com.sciatta.hummer.core.fs.directory.FSImage;
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
 * Created by Rain on 2022/10/25<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 向元数据节点同步上传镜像；上传完成后结束连接，即短连接方式上传
 */
public class FSImageUploader {

    private static final Logger logger = LoggerFactory.getLogger(FSImageUploader.class);

    private final FSImage fsImage;

    public FSImageUploader(FSImage fsImage) {
        this.fsImage = fsImage;
    }

    /**
     * 镜像上传
     */
    public void upload() throws IOException {
        Selector selector = null;
        SocketChannel channel = null;

        try {
            selector = Selector.open();
            channel = SocketChannel.open();
            channel.configureBlocking(false);
            channel.register(selector, SelectionKey.OP_CONNECT);

            channel.connect(new InetSocketAddress(
                    BackupNodeConfig.getNameNodeImageUploadHost(), BackupNodeConfig.getNameNodeImageUploadPort()));

            boolean uploading = true;

            while (uploading) {
                selector.select();  // 阻塞

                Iterator<SelectionKey> keysIterator = selector.selectedKeys().iterator();
                while (keysIterator.hasNext()) {
                    SelectionKey key = keysIterator.next();
                    keysIterator.remove();

                    if (key.isConnectable()) {

                        channel = (SocketChannel) key.channel();

                        if (channel.isConnectionPending()) {
                            channel.finishConnect();

                            // maxTxId(8) + timestamp(8) + directory length(4) + directory(N)
                            int bufferSize = 8 + 8 + 4 + fsImage.getDirectory().getBytes().length;
                            ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
                            buffer.putLong(fsImage.getMaxTxId());
                            buffer.putLong(fsImage.getTimestamp());
                            buffer.putInt(fsImage.getDirectory().getBytes().length);
                            buffer.put(fsImage.getDirectory().getBytes());

                            buffer.flip();
                            channel.write(buffer);

                            logger.debug("upload fsImage to name node, size is {}", buffer.capacity());
                        }

                        channel.register(selector, SelectionKey.OP_READ);

                    } else if (key.isReadable()) {
                        ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024);
                        channel = (SocketChannel) key.channel();
                        int count = channel.read(buffer);

                        if (count > 0) {
                            logger.debug("receive {} response from name node while upload fsImage", new String(buffer.array(), 0, count));
                            uploading = false;
                        }
                    }
                }
            }
        } finally {
            if (channel != null) {
                try {
                    channel.close();
                } catch (IOException ignored) {
                }
            }

            if (selector != null) {
                try {
                    selector.close();
                } catch (IOException ignored) {
                }
            }
        }
    }
}
