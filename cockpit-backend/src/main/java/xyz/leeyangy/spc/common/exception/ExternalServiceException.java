package xyz.leeyangy.spc.common.exception;

import xyz.leeyangy.spc.common.StatusCode;

/**
 * 外部服务调用异常。
 * 用于调用第三方接口（如企业微信）失败的场景，携带服务名便于定位。
 */
public class ExternalServiceException extends BusinessException {

    public ExternalServiceException(String serviceName, String message) {
        super(StatusCode.SYSTEM_ERROR, "[" + serviceName + "] " + message);
    }

    public ExternalServiceException(String serviceName, String message, Throwable cause) {
        super(StatusCode.SYSTEM_ERROR, "[" + serviceName + "] " + message, cause);
    }
}
