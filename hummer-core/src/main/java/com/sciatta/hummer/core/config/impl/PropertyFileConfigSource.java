package com.sciatta.hummer.core.config.impl;

import com.sciatta.hummer.core.config.AbstractConfigSource;
import com.sciatta.hummer.core.config.ConfigSource;
import com.sciatta.hummer.core.util.ClassUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;

/**
 * Created by Rain on 2023/1/1<br>
 * All Rights Reserved(C) 2017 - 2023 SCIATTA <br> <p/>
 * 从属性文件加载配置参数
 */
public class PropertyFileConfigSource extends AbstractConfigSource {
    private static final Logger logger = LoggerFactory.getLogger(PropertyFileConfigSource.class);

    /**
     * 属性文件
     */
    private static final String PROPERTY_FILE = "hummer.properties";

    public PropertyFileConfigSource() {
        super();
        Properties properties = loadProperties();
        toCache(properties);
    }

    public PropertyFileConfigSource(ConfigSource delegate) {
        super(delegate);
        Properties properties = loadProperties();
        toCache(properties);
    }

    /**
     * 加载的属性设置缓存
     *
     * @param properties 加载的属性
     */
    private void toCache(Properties properties) {
        properties.forEach((k, v) -> {
            configCache.put((String) k, new Holder<>(v));
        });
    }

    /**
     * 从属性文件加载配置
     */
    private Properties loadProperties() {
        Properties properties = new Properties();

        List<URL> list = new ArrayList<>();
        try {
            Enumeration<URL> urls = ClassUtils.getClassLoader().getResources(PROPERTY_FILE);
            while (urls.hasMoreElements()) {
                list.add(urls.nextElement());
            }
        } catch (Throwable e) {
            logger.warn("{} while load properties file {}", e.getMessage(), PROPERTY_FILE);
        }

        if (list.isEmpty()) {
            logger.warn("no {} found on the class path", PROPERTY_FILE);
            return properties;
        }

        if (list.size() > 1) {
            logger.warn("only 1 {} file is expected, but {} files found on class path {}",
                    PROPERTY_FILE, list.size(), list);
            return properties;
        }

        URL url = list.get(0);
        logger.info("load {} properties file from {}", PROPERTY_FILE, url);

        try {
            InputStream input = url.openStream();
            if (input != null) {
                try {
                    properties.load(input);
                } finally {
                    try {
                        input.close();
                    } catch (Throwable ignore) {
                    }
                }
            }
        } catch (Throwable e) {
            logger.warn("{} while load properties file {}", e.getMessage(), url);
        }

        return properties;
    }

    @Override
    protected Object loadConfig(String configName) {
        return INVALID;
    }
}
