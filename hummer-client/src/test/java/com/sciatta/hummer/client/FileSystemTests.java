package com.sciatta.hummer.client;

import org.junit.Test;

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
}
