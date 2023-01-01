package com.sciatta.hummer.core.config;

import com.sciatta.hummer.core.config.impl.PropertyFileConfigSource;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by Rain on 2023/1/1<br>
 * All Rights Reserved(C) 2017 - 2023 SCIATTA <br> <p/>
 * PropertyFileConfigSourceTests
 */
public class PropertyFileConfigSourceTests {
    @Test
    public void testGetIntConfig() {
        ConfigSource configSource = new PropertyFileConfigSource();
        int test = configSource.getIntConfig("interval", 0);
        assertEquals(10, test);

        test = configSource.getIntConfig("interval", 0);
        assertEquals(10, test);

        test = configSource.getIntConfig("notHave", 100);
        assertEquals(100, test);
    }
}
