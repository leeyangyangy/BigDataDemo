package xyz.leeyangy.spc.common.exception;

import xyz.leeyangy.spc.common.StatusCode;

/**
 * 业务状态冲突异常。
 * 用于操作与当前业务状态冲突的场景（如删除生效中的版本、状态机非法迁移）。
 */
public class BusinessStateException extends BusinessException {

    public BusinessStateException(String message) {
        super(StatusCode.CONFLICT, message);
    }
}
