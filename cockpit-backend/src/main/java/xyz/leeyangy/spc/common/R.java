package xyz.leeyangy.spc.common;

import lombok.Data;

import java.io.Serializable;

@Data
public class R<T> implements Serializable {

    private int code;
    private String msg;
    private T data;

    private R() {}

    public static <T> R<T> ok() {
        R<T> r = new R<>();
        r.code = StatusCode.SUCCESS;
        r.msg = StatusMsg.SUCCESS;
        return r;
    }

    public static <T> R<T> ok(T data) {
        R<T> r = ok();
        r.data = data;
        return r;
    }

    public static <T> R<T> ok(String msg, T data) {
        R<T> r = new R<>();
        r.code = StatusCode.SUCCESS;
        r.msg = msg;
        r.data = data;
        return r;
    }

    public static <T> R<T> fail(String msg) {
        R<T> r = new R<>();
        r.code = StatusCode.FAIL;
        r.msg = msg;
        return r;
    }

    public static <T> R<T> fail(int code, String msg) {
        R<T> r = new R<>();
        r.code = code;
        r.msg = msg;
        return r;
    }

    public boolean isSuccess() {
        return code == StatusCode.SUCCESS;
    }
}
