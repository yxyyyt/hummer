package com.sciatta.hummer.core.server;

/**
 * Created by Rain on 2023/1/10<br>
 * All Rights Reserved(C) 2017 - 2023 SCIATTA <br> <p/>
 * 持有值帮助类，方便同步
 */
public class Holder<T> {

    private volatile T value;

    public Holder() {
    }

    public Holder(T value) {
        this.value = value;
    }

    public void set(T value) {
        this.value = value;
    }

    public T get() {
        return value;
    }
}
