package com.sciatta.hummer.core.fs.directory;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by Rain on 2022/12/19<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * FSDirectoryTests
 */
public class FSDirectoryTests {
    @Test
    public void testMkdir() {
        FSDirectory fsDirectory = new FSDirectory();
        fsDirectory.mkdir(1, "/hummer/test");

        INodeDirectory dirTree = fsDirectory.getDirTree();
        System.out.println(dirTree);
    }

    @Test
    public void testCreateFileNotExistsParentDirectory() {
        FSDirectory fsDirectory = new FSDirectory();
        boolean test = fsDirectory.createFile(1, "/hummer/test/1.txt");
        assertFalse(test);
    }

    @Test
    public void testCreateFileSuccess() {
        FSDirectory fsDirectory = new FSDirectory();
        fsDirectory.mkdir(1, "/hummer/test");
        boolean test = fsDirectory.createFile(2, "/hummer/test/1.txt");
        assertTrue(test);
    }

    @Test
    public void testCreateFileDuplicatedFile() {
        FSDirectory fsDirectory = new FSDirectory();
        fsDirectory.mkdir(1, "/hummer/test");
        boolean test = fsDirectory.createFile(2, "/hummer/test/1.txt");
        assertTrue(test);

        test = fsDirectory.createFile(3, "/hummer/test/1.txt");
        assertFalse(test);
    }
}
