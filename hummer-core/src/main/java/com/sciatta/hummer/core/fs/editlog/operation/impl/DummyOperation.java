package com.sciatta.hummer.core.fs.editlog.operation.impl;

import com.sciatta.hummer.core.fs.editlog.operation.Operation;
import com.sciatta.hummer.core.fs.editlog.operation.OperationType;

/**
 * Created by Rain on 2022/10/23<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 空操作
 */
public class DummyOperation extends Operation {
    public DummyOperation() {
        super(OperationType.DUMMY);
    }

    @Override
    public String toString() {
        return "DummyOperation{" +
                "type=" + type +
                '}';
    }
}
