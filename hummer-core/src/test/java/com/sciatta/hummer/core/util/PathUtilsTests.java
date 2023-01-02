package com.sciatta.hummer.core.util;

import com.sciatta.hummer.core.fs.editlog.FlushedSegment;
import org.junit.Test;

import static org.junit.Assert.*;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by Rain on 2022/12/22<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * PathUtilsTests
 */
public class PathUtilsTests {

    @Test
    public void testGetUserHome() {
        String userHome = PathUtils.getUserHome();
        System.out.println(userHome);
    }

    @Test
    public void testGetFlushedSegmentFromEditsLogFile() {
        Path path = Paths.get("/hummer/data/editslog/edits-1-1.log");

        FlushedSegment test = PathUtils.getFlushedSegmentFromEditsLogFile(path);
        assertNotNull(test);
        assertEquals(1, test.getMinTxId());
        assertEquals(1, test.getMinTxId());

        path = Paths.get("/hummer/data/editslog/-1-1.log");
        test = PathUtils.getFlushedSegmentFromEditsLogFile(path);
        assertNull(test);

        Paths.get("/hummer/data/editslog/edits-1-1");
        test = PathUtils.getFlushedSegmentFromEditsLogFile(path);
        assertNull(test);

        Paths.get("/hummer/data/editslog/edits-1.log");
        test = PathUtils.getFlushedSegmentFromEditsLogFile(path);
        assertNull(test);

        Paths.get("/hummer/data/editslog/edits-1-b.log");
        test = PathUtils.getFlushedSegmentFromEditsLogFile(path);
        assertNull(test);
    }

    @Test
    public void testIsValidEditsLogFile() {
        boolean test = PathUtils.isValidEditsLogFile(Paths.get("/hummer/data/editslog/edits-1-1.log"));
        assertTrue(test);

        test = PathUtils.isValidEditsLogFile(Paths.get("/hummer/data/editslog/edits-A-1.log"));
        assertFalse(test);
    }

    @Test
    public void testGetPathWithSlashAtLast() {
        String path = "D:\\hummer\\data\\editslog";
        String test = PathUtils.getPathWithSlashAtLast(path);
        System.out.println(test);

        path = "D:\\hummer\\data\\editslog\\";
        test = PathUtils.getPathWithSlashAtLast(path);
        System.out.println(test);
    }
}
