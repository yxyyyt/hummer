package com.sciatta.hummer.client.api;

import com.sciatta.hummer.client.api.FileSystem;
import com.sciatta.hummer.client.api.impl.FileSystemImpl;
import com.sciatta.hummer.core.transport.TransportStatus;
import com.sciatta.hummer.core.util.PathUtils;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by Rain on 2022/12/13<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * FileSystemTests
 */
public class FileSystemTests {
    @Test
    public void testMkdir() {
        FileSystem fileSystem = new FileSystemImpl();

        int result = fileSystem.mkdir("/hummer/images");

        assertEquals(TransportStatus.Mkdir.SUCCESS, result);
    }

    @Test
    public void testUploadFile() throws IOException {
        FileSystem fileSystem = new FileSystemImpl();

        byte[] bytes = Files.readAllBytes(getUploadFile());

        for (int i = 0; i < 10; i++) {
            boolean ans = fileSystem.uploadFile(bytes, "/hummer/images/face" + i + ".jpg", bytes.length);
            assertTrue(ans);
        }
    }

    @Test
    public void testDownloadFile() throws IOException {
        FileSystem fileSystem = new FileSystemImpl();

        for (int i = 0; i < 10; i++) {
            String fileName = "face" + i + ".jpg";
            byte[] bytes = fileSystem.downloadFile("/hummer/images/" + fileName);
            putDownloadFile(bytes, fileName);
        }
    }

    private Path getUploadFile() throws IOException {
        String path = PathUtils.getUserHome() + PathUtils.getFileSeparator() +
                "hummer" + PathUtils.getFileSeparator() +
                "upload" + PathUtils.getFileSeparator() +
                "face.jpg";
        return PathUtils.getPathAndCreateDirectoryIfNotExists(path);
    }

    public void putDownloadFile(byte[] bytes, String fileName) throws IOException {
        String path = PathUtils.getUserHome() + PathUtils.getFileSeparator() +
                "hummer" + PathUtils.getFileSeparator() +
                "download" + PathUtils.getFileSeparator() +
                "new_" + fileName;

        Path pathAndCreateDirectoryIfNotExists = PathUtils.getPathAndCreateDirectoryIfNotExists(path);
        Files.write(pathAndCreateDirectoryIfNotExists, bytes);
    }

    @Test
    public void testBatchMkdir() {
        int threadNum = 5;
        AtomicInteger id = new AtomicInteger();

        FileSystem fileSystem = new FileSystemImpl();
        ExecutorService pool = Executors.newFixedThreadPool(threadNum, r -> {
            Thread thread = new Thread(r);
            thread.setName("thread" + id.incrementAndGet());
            return thread;
        });

        CountDownLatch latch = new CountDownLatch(threadNum);

        for (int i = 0; i < threadNum; i++) {
            pool.execute(() -> {
                for (int j = 0; j < 200; j++) {
                    fileSystem.mkdir("/hummer/test/" + Thread.currentThread().getName() + "/" + Math.random());
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                latch.countDown();
            });
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testShutdown() {
        FileSystem fileSystem = new FileSystemImpl();

        int result = fileSystem.shutdown(); // 发起元数据节点发起停机请求

        assertEquals(1, result);
    }
}
