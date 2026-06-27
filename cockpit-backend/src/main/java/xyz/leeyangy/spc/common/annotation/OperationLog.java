package xyz.leeyangy.spc.common.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 操作日志注解：标注在 Controller 方法上，由 {@code OperationLogAspect} 切面统一记录操作日志。
 *
 * <p>支持 SpEL 表达式动态生成日志内容、目标 ID 与操作结果：
 * <ul>
 *   <li>方法参数：按参数名引用，如 {@code #req.empNo}</li>
 *   <li>返回值：在成功返回时可用 {@code #result}，如 {@code #result.data.id}</li>
 *   <li>路径变量 id：可用 {@code #id} 简写（等同于 {@code @PathVariable("id") Long id}）</li>
 * </ul>
 *
 * <p>结果判定规则（未指定 {@link #resultExpression()} 时）：
 * <ul>
 *   <li>方法正常返回且返回值为 {@code R}：{@code isSuccess()} 为 true 记 SUCCESS，否则记 FAIL（msg 作为 errorMsg）</li>
 *   <li>方法正常返回且返回值非 {@code R}：记 SUCCESS</li>
 *   <li>方法抛出异常：记 FAIL（异常 message 作为 errorMsg）</li>
 * </ul>
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface OperationLog {

    /** 业务模块，例如 USER / WORKSHOP / SPC_DATA */
    String module();

    /** 操作动作，例如 CREATE / UPDATE / DELETE / LOGIN */
    String action();

    /** 目标对象类型，例如 SysUser / Workshop。默认空字符串 */
    String targetType() default "";

    /**
     * 日志内容 SpEL 表达式。为空时使用 "module action" 默认内容。
     * 例如："'创建车间: ' + #workshop.workshopCode + ' - ' + #workshop.workshopName"
     */
    String content() default "";

    /**
     * 目标对象 ID 的 SpEL 表达式，返回 Long 或可转 Long 的值。为空时按以下顺序自动推断：
     * <ol>
     *   <li>方法参数中标注 {@code @PathVariable("id")} 的值</li>
     *   <li>返回值为 {@code R<T>} 时，T 中存在 {@code getId()} 方法则取其返回值</li>
     * </ol>
     * 例如："#workshop.id" 或 "#result.data.id"
     */
    String targetId() default "";

    /**
     * 操作结果 SpEL 表达式，返回字符串 SUCCESS / FAIL / PARTIAL。为空时按切面默认规则判定。
     * 例如：{@code "#result.data.size() == #dataList.size() ? 'SUCCESS' : 'PARTIAL'"}
     */
    String resultExpression() default "";
}
