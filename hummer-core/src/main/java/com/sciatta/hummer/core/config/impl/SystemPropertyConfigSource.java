package com.sciatta.hummer.core.config.impl;

import com.sciatta.hummer.core.config.AbstractConfigSource;
import com.sciatta.hummer.core.config.ConfigSource;

/**
 * Created by Rain on 2023/1/1<br>
 * All Rights Reserved(C) 2017 - 2023 SCIATTA <br> <p/>
 * 从系统属性（-Dx=y）加载配置参数
 */
public class SystemPropertyConfigSource extends AbstractConfigSource {
    public SystemPropertyConfigSource() {
        super();
    }

    public SystemPropertyConfigSource(ConfigSource delegate) {
        super(delegate);
    }

    @Override
    protected Object loadConfig(String configName) {
        Object configValue = System.getProperty(configName);

        return configValue == null ? INVALID : configValue;
    }
}
