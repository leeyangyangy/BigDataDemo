package xyz.leeyangy.spc.common.exception;

import xyz.leeyangy.spc.common.StatusCode;

/**
 * 资源不存在异常。
 * 用于查询数据库未命中预期记录的场景，对应 HTTP 语义的资源不存在。
 */
public class ResourceNotFoundException extends BusinessException {

    public ResourceNotFoundException(String resourceName, Object identifier) {
        super(StatusCode.DATA_NOT_FOUND, resourceName + "不存在: " + identifier);
    }

    public ResourceNotFoundException(String message) {
        super(StatusCode.DATA_NOT_FOUND, message);
    }
}
