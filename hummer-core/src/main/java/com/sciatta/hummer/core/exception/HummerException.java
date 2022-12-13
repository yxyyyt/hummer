package com.sciatta.hummer.core.exception;

/**
 * Created by Rain on 2022/12/6<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * 系统运行时异常
 */
public class HummerException extends RuntimeException {
    public HummerException() {
        super();
    }

    public HummerException(String message, Object... args) {
        super(String.format(message, args));
    }

    public HummerException(String message, Throwable cause, Object... args) {
        super(String.format(message, args), cause);
    }

    public HummerException(Throwable cause) {
        super(cause);
    }
}
