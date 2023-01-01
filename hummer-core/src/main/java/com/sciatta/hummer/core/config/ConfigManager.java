package com.sciatta.hummer.core.config;

import com.sciatta.hummer.core.config.impl.PropertyFileConfigSource;
import com.sciatta.hummer.core.config.impl.SystemPropertyConfigSource;

/**
 * Created by Rain on 2023/1/1<br>
 * All Rights Reserved(C) 2017 - 2023 SCIATTA <br> <p/>
 * 配置管理器
 */
public final class ConfigManager implements ConfigSource {
    private volatile static ConfigManager INSTANCE;

    private final ConfigSource configSource;

    private ConfigManager() {
        this.configSource = new SystemPropertyConfigSource(new PropertyFileConfigSource());
    }

    /**
     * 获取配置管理器单例
     *
     * @return 配置管理器
     */
    public static ConfigManager getInstance() {
        if (INSTANCE == null) {
            synchronized (ConfigManager.class) {
                if (INSTANCE == null) {
                    INSTANCE = new ConfigManager();
                }
            }
        }

        return INSTANCE;
    }


    @Override
    public boolean getBooleanConfig(String configName, boolean defaultValue) {
        return this.configSource.getBooleanConfig(configName, defaultValue);
    }

    @Override
    public int getIntConfig(String configName, int defaultValue) {
        return this.configSource.getIntConfig(configName, defaultValue);
    }

    @Override
    public long getLongConfig(String configName, long defaultValue) {
        return this.configSource.getLongConfig(configName, defaultValue);
    }

    @Override
    public float getFloatConfig(String configName, float defaultValue) {
        return this.configSource.getFloatConfig(configName, defaultValue);
    }

    @Override
    public double getDoubleConfig(String configName, double defaultValue) {
        return this.configSource.getDoubleConfig(configName, defaultValue);
    }

    @Override
    public String getStringConfig(String configName, String defaultValue) {
        return this.configSource.getStringConfig(configName, defaultValue);
    }
}
