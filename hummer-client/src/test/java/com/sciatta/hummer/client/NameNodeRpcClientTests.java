package com.sciatta.hummer.client;

import com.sciatta.hummer.client.rpc.NameNodeRpcClient;
import com.sciatta.hummer.core.fs.DataNodeInfo;
import org.junit.Test;

import static org.junit.Assert.*;

import java.util.List;

/**
 * Created by Rain on 2022/12/31<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * NameNodeRpcClientTests
 */
public class NameNodeRpcClientTests {
    @Test
    public void testAllocateDataNodes_Invalid() {
        NameNodeRpcClient client = new NameNodeRpcClient();
        List<DataNodeInfo> dataNodes = client.allocateDataNodes("/humer/test/1.log", 100);
        assertNull(dataNodes);
    }

    @Test
    public void testAllocateDataNodes_Success() {
        NameNodeRpcClient client = new NameNodeRpcClient();
        List<DataNodeInfo> dataNodes = client.allocateDataNodes("/humer/test/1.log", 100);
        assertEquals(2, dataNodes.size());
    }
}
