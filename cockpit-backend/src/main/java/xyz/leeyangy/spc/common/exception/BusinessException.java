package xyz.leeyangy.spc.common.exception;

import lombok.Getter;
import xyz.leeyangy.spc.common.StatusCode;

/**
 * 业务异常基类。
 * 所有可预期、需向用户暴露明确提示的业务错误均应抛出本类或其子类，
 * 由 {@link xyz.leeyangy.spc.config.GlobalExceptionAdvice} 统一捕获并转换为响应。
 */
@Getter
public class BusinessException extends RuntimeException {

    private final int code;

    public BusinessException(String message) {
        super(message);
        this.code = StatusCode.FAIL;
    }

    public BusinessException(int code, String message) {
        super(message);
        this.code = code;
    }

    public BusinessException(int code, String message, Throwable cause) {
        super(message, cause);
        this.code = code;
    }
}
