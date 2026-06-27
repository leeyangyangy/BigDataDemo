package xyz.leeyangy.spc.common.exception;

import xyz.leeyangy.spc.common.StatusCode;

/**
 * 参数校验异常。
 * 用于业务前置校验失败（必填项为空、格式非法、超出合理范围等）。
 */
public class ParamValidationException extends BusinessException {

    public ParamValidationException(String message) {
        super(StatusCode.PARAM_ERROR, message);
    }
}
