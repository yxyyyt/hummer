package com.sciatta.hummer.core.config;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by Rain on 2023/1/1<br>
 * All Rights Reserved(C) 2017 - 2023 SCIATTA <br> <p/>
 * ConfigManagerTests
 */
public class ConfigManagerTests {
    @Test
    public void testGetPropertyFile() {
        ConfigManager configManager = ConfigManager.getInstance();
        int test = configManager.getIntConfig("interval", 0);
        assertEquals(10, test);
    }

    @Test
    public void testGetSystemProperty() {
        ConfigManager configManager = ConfigManager.getInstance();
        int test = configManager.getIntConfig("interval", 0);
        assertEquals(100, test);
    }

    @Test
    public void testGetDefaultValue() {
        ConfigManager configManager = ConfigManager.getInstance();
        int test = configManager.getIntConfig("interval1", 0);
        assertEquals(0, test);
    }
}
