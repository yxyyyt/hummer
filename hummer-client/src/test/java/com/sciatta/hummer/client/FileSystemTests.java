package com.sciatta.hummer.client;

import com.sciatta.hummer.client.fs.FileSystem;
import com.sciatta.hummer.client.fs.FileSystemImpl;
import com.sciatta.hummer.core.util.PathUtils;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
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
        int result = fileSystem.mkdir("/hummer/init");
        assertEquals(1, result);
    }

    @Test
    public void testUploadFile() throws IOException {
        FileSystem fileSystem = new FileSystemImpl();
        fileSystem.mkdir("/hummer/images");

        byte[] bytes = Files.readAllBytes(getTestFile());
        boolean ans = fileSystem.uploadFile(bytes, "/hummer/images/" + Math.random() + ".jpg", bytes.length);
        assertTrue(ans);
    }

    private Path getTestFile() throws IOException {
        String path = PathUtils.getUserHome() + PathUtils.getFileSeparator() +
                "hummer" + PathUtils.getFileSeparator() +
                "face.jpg";
        return PathUtils.getPathAndCreateDirectoryIfNotExists(path);
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
