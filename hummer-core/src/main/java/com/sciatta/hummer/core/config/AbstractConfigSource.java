package com.sciatta.hummer.core.config;

import com.sciatta.hummer.core.server.Holder;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by Rain on 2023/1/1<br>
 * All Rights Reserved(C) 2017 - 2023 SCIATTA <br> <p/>
 * 抽象配置数据源
 */
public abstract class AbstractConfigSource implements ConfigSource {
    /**
     * 配置参数缓存；
     * 如果没有加载参数，则Holder中持有的是null；如果加载过参数，但没有有效参数，则Holder中持有的是INVALID对象
     */
    protected final ConcurrentMap<String, Holder<Object>> configCache = new ConcurrentHashMap<>();

    /**
     * 委托加载配置数据源
     */
    private ConfigSource delegate;

    /**
     * 无效参数占用符
     */
    protected static final Object INVALID = new Object();

    public AbstractConfigSource() {
    }

    public AbstractConfigSource(ConfigSource delegate) {
        this.delegate = delegate;
    }

    @Override
    public boolean getBooleanConfig(String configName, boolean defaultValue) {
        Object value = getConfigValue(configName);

        if (value != INVALID) {
            return Boolean.parseBoolean((String) value);
        }

        if (this.delegate != null) {
            return this.delegate.getBooleanConfig(configName, defaultValue);
        }

        return defaultValue;
    }

    @Override
    public int getIntConfig(String configName, int defaultValue) {
        Object value = getConfigValue(configName);

        if (value != INVALID) {
            return Integer.parseInt((String) value);
        }

        if (this.delegate != null) {
            return this.delegate.getIntConfig(configName, defaultValue);
        }

        return defaultValue;
    }

    @Override
    public long getLongConfig(String configName, long defaultValue) {
        Object value = getConfigValue(configName);

        if (value != INVALID) {
            return Long.parseLong((String) value);
        }

        if (this.delegate != null) {
            return this.delegate.getLongConfig(configName, defaultValue);
        }

        return defaultValue;
    }

    @Override
    public float getFloatConfig(String configName, float defaultValue) {
        Object value = getConfigValue(configName);

        if (value != INVALID) {
            return Float.parseFloat((String) value);
        }

        if (this.delegate != null) {
            return this.delegate.getFloatConfig(configName, defaultValue);
        }

        return defaultValue;
    }

    @Override
    public double getDoubleConfig(String configName, double defaultValue) {
        Object value = getConfigValue(configName);

        if (value != INVALID) {
            return Double.parseDouble((String) value);
        }

        if (this.delegate != null) {
            return this.delegate.getDoubleConfig(configName, defaultValue);
        }

        return defaultValue;
    }

    @Override
    public String getStringConfig(String configName, String defaultValue) {
        Object value = getConfigValue(configName);

        if (value != INVALID) {
            return (String) value;
        }

        if (this.delegate != null) {
            return this.delegate.getStringConfig(configName, defaultValue);
        }

        return defaultValue;
    }

    /**
     * 延迟加载参数
     *
     * @param configName 参数名
     * @return 参数值；如果当前数据源加载配置后是无效参数，则返回INVALID对象
     */
    protected abstract Object loadConfig(String configName);

    /**
     * 获取参数值
     *
     * @param configName 参数名
     * @return 参数值
     */
    private Object getConfigValue(String configName) {
        Holder<Object> holder = configCache.getOrDefault(configName, new Holder<>());

        if (holder.get() == null) {
            synchronized (holder) {
                if (holder.get() == null) {
                    Object v = loadConfig(configName);
                    holder.set(v);
                }
            }
        }

        return holder.get();
    }
}
