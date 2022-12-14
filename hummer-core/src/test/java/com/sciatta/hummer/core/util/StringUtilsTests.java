package com.sciatta.hummer.core.util;

import org.junit.Test;

/**
 * Created by Rain on 2022/12/14<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * StringUtilsTests
 */
public class StringUtilsTests {
    @Test
    public void testGetNewLine() {
        String newLine = StringUtils.getNewLine();
        System.out.print("begin");
        System.out.print(newLine);
        System.out.print("end");
    }
}
