package xyz.leeyangy.spc.controller.admin;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.web.bind.annotation.DeleteMapping;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 审计日志控制器单元测试。
 *
 * <p>重点验证 P0 修复: 等保三级要求审计日志不可删除, 验证控制器无 @DeleteMapping 注解。
 */
@DisplayName("审计日志控制器测试 (DELETE 端点已移除)")
class AdminLogControllerTest {

    @Test
    @DisplayName("AdminOperationLogController 无 @DeleteMapping 注解 (等保三级要求)")
    void adminOperationLogController_NoDeleteMappingAnnotation() {
        Method[] methods = AdminOperationLogController.class.getDeclaredMethods();

        long deleteCount = 0;
        for (Method method : methods) {
            if (method.isAnnotationPresent(DeleteMapping.class)) {
                deleteCount++;
                System.err.println("[FAIL] 发现 @DeleteMapping: " + method.getName());
            }
        }

        assertEquals(0, deleteCount,
                "AdminOperationLogController 不应有任何 @DeleteMapping 注解 (等保三级: 审计日志不可删除)");
    }

    @Test
    @DisplayName("AdminLogController 无 @DeleteMapping 注解 (等保三级要求)")
    void adminLogController_NoDeleteMappingAnnotation() {
        Method[] methods = AdminLogController.class.getDeclaredMethods();

        long deleteCount = 0;
        for (Method method : methods) {
            if (method.isAnnotationPresent(DeleteMapping.class)) {
                deleteCount++;
                System.err.println("[FAIL] 发现 @DeleteMapping: " + method.getName());
            }
        }

        assertEquals(0, deleteCount,
                "AdminLogController 不应有任何 @DeleteMapping 注解 (等保三级: 审计日志不可删除)");
    }

    @Test
    @DisplayName("AdminOperationLogController 保留 @GetMapping 查询能力")
    void adminOperationLogController_HasGetMappingForQuery() {
        Method[] methods = AdminOperationLogController.class.getDeclaredMethods();

        long getCount = 0;
        for (Method method : methods) {
            if (method.isAnnotationPresent(
                    org.springframework.web.bind.annotation.GetMapping.class)) {
                getCount++;
            }
        }

        assertTrue(getCount > 0, "应保留 @GetMapping 查询端点");
    }

    @Test
    @DisplayName("AdminLogController 保留 @GetMapping 查询能力")
    void adminLogController_HasGetMappingForQuery() {
        Method[] methods = AdminLogController.class.getDeclaredMethods();

        long getCount = 0;
        for (Method method : methods) {
            if (method.isAnnotationPresent(
                    org.springframework.web.bind.annotation.GetMapping.class)) {
                getCount++;
            }
        }

        assertTrue(getCount > 0, "应保留 @GetMapping 查询端点");
    }
}
