package com.sciatta.hummer.client;

import com.sciatta.hummer.client.rpc.NameNodeRpcClient;
import com.sciatta.hummer.core.fs.data.DataNodeInfo;
import com.sciatta.hummer.core.server.Holder;
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
    public void testAllocateDataNodes_No_DataNode() {
        NameNodeRpcClient client = new NameNodeRpcClient();
        Holder<List<DataNodeInfo>> holder = new Holder<>();
        client.allocateDataNodes("/hummer/test/1.log", holder);
        assertNull(holder.get());
    }

    @Test
    public void testAllocateDataNodes_Success() {
        NameNodeRpcClient client = new NameNodeRpcClient();
        Holder<List<DataNodeInfo>> holder = new Holder<>();
        client.allocateDataNodes("/hummer/test/1.log", holder);
        assertEquals(2, holder.get().size());
    }
}
