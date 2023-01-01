package com.sciatta.hummer.core.config;

import com.sciatta.hummer.core.config.impl.SystemPropertyConfigSource;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by Rain on 2023/1/1<br>
 * All Rights Reserved(C) 2017 - 2023 SCIATTA <br> <p/>
 * SystemPropertyConfigSourceTests
 */
public class SystemPropertyConfigSourceTests {
    @Test
    public void testGetIntConfig() {
        ConfigSource configSource = new SystemPropertyConfigSource();
        int test = configSource.getIntConfig("port", 0);   // -Dport=8080
        assertEquals(8080, test);
    }
}
