package com.sciatta.hummer.core.server;

/**
 * Created by Rain on 2023/1/10<br>
 * All Rights Reserved(C) 2017 - 2023 SCIATTA <br> <p/>
 * 持有值帮助类
 */
public class Holder<T> {

    private volatile T value;

    public void set(T value) {
        this.value = value;
    }

    public T get() {
        return value;
    }
}
