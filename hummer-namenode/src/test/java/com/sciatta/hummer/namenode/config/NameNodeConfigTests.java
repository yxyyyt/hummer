package com.sciatta.hummer.namenode.config;

import org.junit.Test;

/**
 * Created by Rain on 2023/1/2<br>
 * All Rights Reserved(C) 2017 - 2023 SCIATTA <br> <p/>
 * NameNodeConfigTests
 */
public class NameNodeConfigTests {
    @Test
    public void testGetEditsLogBufferLimit() {
        int editsLogBufferLimit = NameNodeConfig.getEditsLogBufferLimit();
        System.out.println(editsLogBufferLimit);
    }

    @Test
    public void testGetNameNodeRootPath() {
        String nameNodeRootPath = NameNodeConfig.getNameNodeRootPath();
        System.out.println(nameNodeRootPath);
    }

    @Test
    public void testGetEditsLogPath() {
        String editsLogPath = NameNodeConfig.getEditsLogPath();
        System.out.println(editsLogPath);
    }

    @Test
    public void testGetCheckpointPath() {
        String checkpointPath = NameNodeConfig.getCheckpointPath();
        System.out.println(checkpointPath);
    }

    @Test
    public void testGetRuntimeRepositoryPath() {
        String runtimeRepositoryPath = NameNodeConfig.getRuntimeRepositoryPath();
        System.out.println(runtimeRepositoryPath);
    }
}
