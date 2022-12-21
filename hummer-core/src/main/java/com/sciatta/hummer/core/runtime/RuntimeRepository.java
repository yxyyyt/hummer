package com.sciatta.hummer.core.runtime;

import com.sciatta.hummer.core.fs.FSNameSystem;
import com.sciatta.hummer.core.util.GsonUtils;
import com.sciatta.hummer.core.util.PathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by Rain on 2022/12/20<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 运行时仓库，管理运行时数据
 */
public class RuntimeRepository {
    private static final Logger logger = LoggerFactory.getLogger(RuntimeRepository.class);

    /**
     * 运行时仓库
     */
    private final ConcurrentMap<String, Object> repository = new ConcurrentHashMap<>();

    private final FSNameSystem fsNameSystem;

    public RuntimeRepository(FSNameSystem fsNameSystem) {
        this.fsNameSystem = fsNameSystem;
    }

    /**
     * 持久化运行时数据
     *
     * @throws IOException IO异常
     */
    public void save() throws IOException {
        Path path = PathUtils.getRuntimeRepositoryFile(this.fsNameSystem.getRuntimeRepositoryPath());

        try (FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            fileChannel.write(ByteBuffer.wrap(GsonUtils.toJson(repository).getBytes()));
            fileChannel.force(false);   // 强制刷写到磁盘
            logger.debug("runtime repository save repository to {}", path.toFile().getPath());
        } catch (IOException e) {
            logger.error("{} while runtime repository save", e.getMessage());
            throw e;
        }
    }

    /**
     * 恢复运行时数据
     *
     * @throws IOException IO异常
     */
    @SuppressWarnings("unchecked")
    public void restore() throws IOException {
        Path path = PathUtils.getRuntimeRepositoryFile(this.fsNameSystem.getRuntimeRepositoryPath());

        if (!Files.exists(path)) {
            logger.debug("not exists {}, runtime repository does not need to be restored", path.toFile().getPath());
            return;
        }

        byte[] bytes = Files.readAllBytes(path);
        repository.putAll(GsonUtils.fromJson(new String(bytes, 0, bytes.length), ConcurrentMap.class));
        logger.debug("runtime repository restore repository from {}", path.toFile().getPath());
    }

    public void setParameter(String name, Object value) {
        repository.put(name, value);
    }

    public boolean getBooleanParameter(String name, boolean defaultValue) {
        return (boolean) repository.getOrDefault(name, defaultValue);
    }

    public char getCharacterParameter(String name, char defaultValue) {
        return (char) repository.getOrDefault(name, defaultValue);
    }

    public int getIntParameter(String name, int defaultValue) {
        return (int) repository.getOrDefault(name, defaultValue);
    }

    public long getLongParameter(String name, long defaultValue) {
        return (long) repository.getOrDefault(name, defaultValue);
    }

    public float getFloatParameter(String name, float defaultValue) {
        return (float) repository.getOrDefault(name, defaultValue);
    }

    public double getDoubleParameter(String name, double defaultValue) {
        return (double) repository.getOrDefault(name, defaultValue);
    }

    @SuppressWarnings("unchecked")
    public <T> List<T> getListParameter(String name, List<T> defaultValue) {
        return Collections.unmodifiableList((List<T>) repository.getOrDefault(name, defaultValue));
    }

    @SuppressWarnings("unchecked")
    public <K, V> Map<K, V> getMapParameter(String name, Map<K, V> defaultValue) {
        return Collections.unmodifiableMap((Map<K, V>) repository.getOrDefault(name, defaultValue));
    }
}
