package com.sciatta.hummer.client;

import com.sciatta.hummer.client.fs.FileSystem;
import com.sciatta.hummer.client.fs.FileSystemImpl;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

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
    public void testCreateFile() {
        FileSystem fileSystem = new FileSystemImpl();
        fileSystem.mkdir("/hummer/init");

        for (int i = 0; i < 10; i++) {
            String fileName = "/hummer/init/" + (int) (Math.random() * 10) + ".test";
            int ans = fileSystem.createFile(fileName);
            System.out.println("[" + (i + 1) + "] create file " + fileName + " result is " + ans);
        }
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
