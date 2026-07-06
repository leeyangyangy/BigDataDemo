package xyz.leeyangy.spc.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.http.HttpStatus;
import org.springframework.validation.BindException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.web.multipart.MaxUploadSizeExceededException;
import xyz.leeyangy.spc.common.R;
import xyz.leeyangy.spc.common.StatusCode;
import xyz.leeyangy.spc.common.constants.OperationLogConstants;
import xyz.leeyangy.spc.common.exception.BusinessException;
import xyz.leeyangy.spc.service.OperationLogService;

import javax.servlet.http.HttpServletRequest;

@Slf4j
@RestControllerAdvice
public class GlobalExceptionAdvice {

    @Autowired
    @Lazy
    private OperationLogService operationLogService;

    /**
     * 业务异常：可预期的业务规则违反（资源不存在、参数校验、状态冲突、外部服务调用失败等）。
     * 携带精确错误码，无需记入操作日志（属正常业务流程，非系统故障）。
     */
    @ExceptionHandler(BusinessException.class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public R<Void> handleBusinessException(BusinessException e, HttpServletRequest request) {
        log.warn("[Business] 业务异常: {} - {}", e.getClass().getSimpleName(), e.getMessage());
        return R.fail(e.getCode(), e.getMessage());
    }

    @ExceptionHandler(Exception.class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public R<Void> handleException(Exception e, HttpServletRequest request) {
        String errorMsg = e.getClass().getSimpleName() + ": " + e.getMessage();
        log.error("[Global] 未捕获异常: {} - {}", e.getClass().getSimpleName(), e.getMessage(), e);
        try {
            operationLogService.record("SYSTEM", "ERROR", null, "EXCEPTION",
                    errorMsg, OperationLogConstants.RESULT_FAIL, getStackTraceSnippet(e), 0,
                    request, null, null);
        } catch (Exception ignored) { }
        return R.fail("系统内部错误，请稍后重试");
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public R<Void> handleValidationException(MethodArgumentNotValidException e) {
        String msg = e.getBindingResult().getFieldErrors().stream()
                .map(fe -> fe.getDefaultMessage())
                .findFirst()
                .orElse("参数校验失败");
        log.warn("[Validation] 参数校验失败: {}", msg);
        return R.fail(StatusCode.PARAM_ERROR, msg);
    }

    @ExceptionHandler(BindException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public R<Void> handleBind(BindException e) {
        String msg = e.getFieldErrors().stream()
                .map(err -> err.getField() + ": " + err.getDefaultMessage())
                .reduce((a, b) -> a + "; " + b)
                .orElse("参数绑定失败");
        return R.fail(400, msg);
    }

    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public R<Void> handleTypeMismatch(MethodArgumentTypeMismatchException e) {
        log.warn("[Global] 参数类型错误: name={} value={}", e.getName(), e.getValue());
        return R.fail(400, "参数 '" + e.getName() + "' 类型错误");
    }

    @ExceptionHandler(MaxUploadSizeExceededException.class)
    @ResponseStatus(HttpStatus.PAYLOAD_TOO_LARGE)
    public R<Void> handleUploadSize(MaxUploadSizeExceededException e) {
        log.warn("[Global] 文件上传超限");
        return R.fail(413, "文件大小超出限制");
    }

    @ExceptionHandler(IllegalArgumentException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public R<Void> handleIllegalArgument(IllegalArgumentException e) {
        log.warn("[Global] 参数非法: {}", e.getMessage());
        return R.fail(400, e.getMessage());
    }

    /**
     * 非业务运行时异常：未预期到的系统级故障（如 AESUtil 加解密失败、NPE 等）。
     * 记入操作日志便于排查, 但不向客户端泄露内部异常细节 (等保三级要求)。
     */
    @ExceptionHandler(RuntimeException.class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public R<Void> handleRuntimeException(RuntimeException e, HttpServletRequest request) {
        log.error("[Global] 系统运行时异常: {} - {}", e.getClass().getSimpleName(), e.getMessage(), e);

        try {
            operationLogService.record("SYSTEM", "RUNTIME_ERROR", null, "EXCEPTION",
                    "系统异常: " + e.getMessage(), OperationLogConstants.RESULT_FAIL, getStackTraceSnippet(e), 0,
                    request, null, null);
        } catch (Exception ignored) { }

        // 不向客户端泄露 e.getMessage(), 仅返回通用提示
        return R.fail(500, "系统内部错误，请稍后重试");
    }

    @ExceptionHandler(SecurityException.class)
    @ResponseStatus(HttpStatus.FORBIDDEN)
    public R<Void> handleSecurity(SecurityException e, HttpServletRequest request) {
        log.warn("[SECURITY_ALERT] 安全异常 (403): uri={} ip={} cause={}",
                request.getRequestURI(), request.getRemoteAddr(), e.getMessage());
        try {
            operationLogService.record("SYSTEM", "SECURITY", null, null,
                    e.getMessage(), OperationLogConstants.RESULT_FAIL, null, 0,
                    request, null, null);
        } catch (Exception ignored) { }
        return R.fail(403, "权限不足");
    }

    private String getStackTraceSnippet(Exception e) {
        StackTraceElement[] stack = e.getStackTrace();
        if (stack == null || stack.length == 0) return "";
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < Math.min(5, stack.length); i++) {
            sb.append("\n    at ").append(stack[i].toString());
        }
        return sb.toString();
    }
}
