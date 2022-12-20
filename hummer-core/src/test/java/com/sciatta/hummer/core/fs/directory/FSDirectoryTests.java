package com.sciatta.hummer.core.fs.directory;

import org.junit.Test;

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
}
