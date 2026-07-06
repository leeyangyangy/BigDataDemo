package xyz.leeyangy.spc.common.aspect;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.AfterReturning;
import org.aspectj.lang.annotation.AfterThrowing;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.core.DefaultParameterNameDiscoverer;
import org.springframework.core.ParameterNameDiscoverer;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;
import org.springframework.web.bind.annotation.PathVariable;
import xyz.leeyangy.spc.common.R;
import xyz.leeyangy.spc.common.annotation.OperationLog;
import xyz.leeyangy.spc.common.constants.OperationLogConstants;
import xyz.leeyangy.spc.common.util.SensitiveDataMasker;
import xyz.leeyangy.spc.service.OperationLogService;

import javax.servlet.http.HttpServletRequest;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Map;

/**
 * 操作日志切面：拦截所有标注 {@link OperationLog} 的方法，统一写入操作日志。
 *
 * <p>切面处理逻辑：
 * <ul>
 *   <li>{@code @AfterReturning}：依据返回值或 {@link OperationLog#resultExpression()} 判定结果</li>
 *   <li>{@code @AfterThrowing}：统一记 FAIL，errorMsg 取异常 message</li>
 * </ul>
 *
 * <p>所有 SpEL 解析异常均被吞掉并降级为默认内容，避免影响业务流程。
 */
@Slf4j
@Aspect
@Component
@RequiredArgsConstructor
public class OperationLogAspect {

    private final OperationLogService operationLogService;
    private final SensitiveDataMasker sensitiveDataMasker;

    private final ExpressionParser parser = new SpelExpressionParser();
    private final ParameterNameDiscoverer paramNameDiscoverer = new DefaultParameterNameDiscoverer();

    @Pointcut("@annotation(operationLog)")
    public void operationLogPointcut(OperationLog operationLog) {
    }

    /**
     * 方法正常返回时记录日志。
     */
    @AfterReturning(pointcut = "operationLogPointcut(operationLog)", returning = "result", argNames = "operationLog,result")
    public void doAfterReturning(JoinPoint joinPoint, OperationLog operationLog, Object result) {
        try {
            EvaluationContext ctx = buildContext(joinPoint, result);
            String content = evalString(operationLog.content(), ctx,
                    operationLog.module() + " " + operationLog.action());
            Long targetId = evalTargetId(joinPoint, result, operationLog.targetId());
            String resultCode = evalResultCode(result, operationLog.resultExpression(), ctx);
            String errorMsg = OperationLogConstants.RESULT_FAIL.equals(resultCode) ? extractErrorMsg(result) : null;

            record(operationLog, targetId, content, resultCode, errorMsg, null, result);
        } catch (Exception e) {
            log.warn("[OperationLog] AfterReturning 处理失败(不影响业务): {}", e.getMessage());
        }
    }

    /**
     * 方法抛出异常时记录失败日志。
     */
    @AfterThrowing(pointcut = "operationLogPointcut(operationLog)", throwing = "ex", argNames = "operationLog,ex")
    public void doAfterThrowing(JoinPoint joinPoint, OperationLog operationLog, Throwable ex) {
        try {
            EvaluationContext ctx = buildContext(joinPoint, null);
            String content = evalString(operationLog.content(), ctx,
                    operationLog.module() + " " + operationLog.action());
            Long targetId = evalTargetId(joinPoint, null, operationLog.targetId());

            record(operationLog, targetId, content, OperationLogConstants.RESULT_FAIL,
                    truncate(ex.getMessage()), ex.getClass().getSimpleName(), null);
        } catch (Exception e) {
            log.warn("[OperationLog] AfterThrowing 处理失败(不影响业务): {}", e.getMessage());
        }
    }

    private void record(OperationLog operationLog, Long targetId, String content,
                        String resultCode, String errorMsg, String errorType, Object result) {
        HttpServletRequest request = currentRequest();
        Long operatorId = extractOperatorId(request);
        if (operatorId == null && result != null) {
            operatorId = extractUserIdFromResult(result);
        }
        // 从 request attribute 提取操作者用户名 (JwtAuthFilter 第 83 行已写入)
        String operatorName = extractOperatorName(request);

        String fullContent = content;
        if (errorType != null && errorMsg != null) {
            fullContent = content + " [" + errorType + ": " + errorMsg + "]";
        }

        // 脱敏处理: 避免 phone/email/idCard/password 等敏感数据明文写入操作日志
        String maskedContent = sensitiveDataMasker.maskContent(fullContent);
        String maskedErrorMsg = sensitiveDataMasker.maskContent(errorMsg);

        operationLogService.record(
                operationLog.module(),
                operationLog.action(),
                targetId,
                operationLog.targetType(),
                maskedContent,
                resultCode,
                maskedErrorMsg,
                0,
                request,
                operatorId,
                operatorName);
    }

    /**
     * 构建 SpEL 上下文：方法参数（按名）+ #result + #id（若存在 PathVariable("id")）。
     */
    private EvaluationContext buildContext(JoinPoint joinPoint, Object result) {
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        Method method = signature.getMethod();
        Object[] args = joinPoint.getArgs();
        String[] paramNames = paramNameDiscoverer.getParameterNames(method);

        StandardEvaluationContext ctx = new StandardEvaluationContext();
        if (paramNames != null) {
            for (int i = 0; i < paramNames.length && i < args.length; i++) {
                ctx.setVariable(paramNames[i], args[i]);
            }
        }

        if (result != null) {
            ctx.setVariable("result", result);
        }

        Long pathId = extractPathVariableId(method, args);
        if (pathId != null) {
            ctx.setVariable("id", pathId);
        }
        return ctx;
    }

    private String evalString(String spel, EvaluationContext ctx, String defaultValue) {
        if (spel == null || spel.isEmpty()) {
            return defaultValue;
        }
        try {
            Expression expression = parser.parseExpression(spel);
            Object value = expression.getValue(ctx);
            return value == null ? defaultValue : value.toString();
        } catch (Exception e) {
            log.debug("[OperationLog] SpEL 解析失败 spel={} cause={}", spel, e.getMessage());
            return defaultValue;
        }
    }

    private Long evalTargetId(JoinPoint joinPoint, Object result, String spel) {
        if (spel != null && !spel.isEmpty()) {
            try {
                Expression expression = parser.parseExpression(spel);
                Object value = expression.getValue(buildContext(joinPoint, result));
                return toLong(value);
            } catch (Exception e) {
                log.debug("[OperationLog] targetId SpEL 解析失败 spel={} cause={}", spel, e.getMessage());
            }
        }
        // 自动推断：PathVariable("id") 优先，否则返回值 R<Entity>.data.id
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        Long pathId = extractPathVariableId(signature.getMethod(), joinPoint.getArgs());
        if (pathId != null) {
            return pathId;
        }
        return extractIdFromResult(result);
    }

    private String evalResultCode(Object result, String spel, EvaluationContext ctx) {
        if (spel != null && !spel.isEmpty()) {
            try {
                Expression expression = parser.parseExpression(spel);
                Object value = expression.getValue(ctx);
                if (value != null) {
                    String s = value.toString();
                    if (OperationLogConstants.RESULT_SUCCESS.equals(s)
                            || OperationLogConstants.RESULT_FAIL.equals(s)
                            || OperationLogConstants.RESULT_PARTIAL.equals(s)) {
                        return s;
                    }
                }
            } catch (Exception e) {
                log.debug("[OperationLog] resultExpression SpEL 解析失败 spel={} cause={}", spel, e.getMessage());
            }
        }
        // 默认规则：R.isFailure → FAIL，否则 SUCCESS
        if (result instanceof R) {
            R<?> r = (R<?>) result;
            return r.isSuccess() ? OperationLogConstants.RESULT_SUCCESS : OperationLogConstants.RESULT_FAIL;
        }
        return OperationLogConstants.RESULT_SUCCESS;
    }

    private String extractErrorMsg(Object result) {
        if (result instanceof R) {
            return ((R<?>) result).getMsg();
        }
        return null;
    }

    private Long extractPathVariableId(Method method, Object[] args) {
        Annotation[][] paramAnnotations = method.getParameterAnnotations();
        for (int i = 0; i < paramAnnotations.length; i++) {
            for (Annotation annotation : paramAnnotations[i]) {
                if (annotation instanceof PathVariable) {
                    PathVariable pv = (PathVariable) annotation;
                    String name = pv.value().isEmpty() ? pv.name() : pv.value();
                    if ("id".equals(name) && i < args.length && args[i] instanceof Long) {
                        return (Long) args[i];
                    }
                }
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private Long extractIdFromResult(Object result) {
        if (!(result instanceof R)) {
            return null;
        }
        Object data = ((R<Object>) result).getData();
        if (data == null) {
            return null;
        }
        // 反射调用 getId()；常见 MyBatis-Plus 实体均含主键 id 字段
        try {
            Method getId = data.getClass().getMethod("getId");
            Object id = getId.invoke(data);
            return toLong(id);
        } catch (NoSuchMethodException ignored) {
            // 实体无 getId() 方法（如 Map、List 等），忽略
        } catch (Exception e) {
            log.debug("[OperationLog] 从返回值提取 id 失败: {}", e.getMessage());
        }
        // 兜底：尝试反射读取字段
        try {
            Field idField = findField(data.getClass(), "id");
            if (idField != null) {
                idField.setAccessible(true);
                return toLong(idField.get(data));
            }
        } catch (Exception e) {
            log.debug("[OperationLog] 从返回值字段提取 id 失败: {}", e.getMessage());
        }
        return null;
    }

    private Field findField(Class<?> clazz, String name) {
        Class<?> c = clazz;
        while (c != null && c != Object.class) {
            try {
                return c.getDeclaredField(name);
            } catch (NoSuchFieldException ignored) {
                c = c.getSuperclass();
            }
        }
        return null;
    }

    private Long toLong(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Long) {
            return (Long) value;
        }
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        try {
            return Long.parseLong(value.toString());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private HttpServletRequest currentRequest() {
        try {
            ServletRequestAttributes attrs =
                    (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
            return attrs == null ? null : attrs.getRequest();
        } catch (Exception e) {
            return null;
        }
    }

    private Long extractOperatorId(HttpServletRequest request) {
        if (request == null) {
            return null;
        }
        Object userId = request.getAttribute("userId");
        return toLong(userId);
    }

    /**
     * 从 request attribute 提取操作者用户名。
     *
     * <p>JwtAuthFilter 在鉴权通过后将 username 写入 request attribute,
     * 切面直接读取即可, 无需再次解析 JWT。</p>
     */
    private String extractOperatorName(HttpServletRequest request) {
        if (request == null) {
            return null;
        }
        Object username = request.getAttribute("username");
        return username == null ? null : username.toString();
    }

    /**
     * 从方法返回值兜底提取 userId（用于登录等未认证场景）。
     *
     * <p>登录接口返回 {@code R<Map>}，Map 含 "userId" 字段；
     * 实体返回场景复用 {@link #extractIdFromResult} 反射取 id。</p>
     */
    private Long extractUserIdFromResult(Object result) {
        if (!(result instanceof R)) {
            return null;
        }
        Object data = ((R<?>) result).getData();
        if (data instanceof Map) {
            return toLong(((Map<?, ?>) data).get("userId"));
        }
        return extractIdFromResult(result);
    }

    private String truncate(String msg) {
        if (msg == null) {
            return null;
        }
        return msg.length() <= 512 ? msg : msg.substring(0, 509) + "...";
    }
}
