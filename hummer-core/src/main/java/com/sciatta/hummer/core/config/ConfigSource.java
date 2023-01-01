package com.sciatta.hummer.core.config;

/**
 * Created by Rain on 2023/1/1<br>
 * All Rights Reserved(C) 2017 - 2023 SCIATTA <br> <p/>
 * 配置数据源
 */
public interface ConfigSource {
    /**
     * 获取布尔参数值
     *
     * @param configName   参数名
     * @param defaultValue 默认值
     * @return 布尔参数值
     */
    boolean getBooleanConfig(String configName, boolean defaultValue);

    /**
     * 获取整型参数值
     *
     * @param configName   参数名
     * @param defaultValue 默认值
     * @return 整型参数值
     */
    int getIntConfig(String configName, int defaultValue);

    /**
     * 获取长整型参数值
     *
     * @param configName   参数名
     * @param defaultValue 默认值
     * @return 长整型参数值
     */
    long getLongConfig(String configName, long defaultValue);

    /**
     * 获取单精度参数值
     *
     * @param configName   参数名
     * @param defaultValue 默认值
     * @return 单精度参数值
     */
    float getFloatConfig(String configName, float defaultValue);

    /**
     * 获取双精度参数值
     *
     * @param configName   参数名
     * @param defaultValue 默认值
     * @return 双精度参数值
     */
    double getDoubleConfig(String configName, double defaultValue);

    /**
     * 获取字符串参数值
     *
     * @param configName   参数名
     * @param defaultValue 默认值
     * @return 字符串参数值
     */
    String getStringConfig(String configName, String defaultValue);
}
