package xyz.leeyangy.spc.config;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.mock.web.MockHttpServletRequest;
import xyz.leeyangy.spc.common.R;
import xyz.leeyangy.spc.common.util.SensitiveDataMasker;
import xyz.leeyangy.spc.service.OperationLogService;

import javax.servlet.http.HttpServletRequest;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.doNothing;

/**
 * 全局异常处理器单元测试。
 *
 * <p>重点验证 P0 修复: handleRuntimeException 不再向客户端泄露 e.getMessage()。
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("全局异常处理器测试 (不泄露内部异常)")
class GlobalExceptionAdviceTest {

    @Mock
    private OperationLogService operationLogService;

    @Mock
    private SensitiveDataMasker sensitiveDataMasker;

    @InjectMocks
    private GlobalExceptionAdvice globalExceptionAdvice;

    @Test
    @DisplayName("handleRuntimeException 返回通用错误信息, 不包含 e.getMessage()")
    void handleRuntimeException_ReturnsGenericMessage_NoExceptionDetail() {
        // 模拟内部异常: 包含敏感的 SQL 信息
        String sensitiveMessage = "SELECT * FROM spc_user WHERE password='admin123' AND id=1";
        RuntimeException ex = new RuntimeException(sensitiveMessage);
        HttpServletRequest request = new MockHttpServletRequest();

        // mock 审计日志记录 (不抛出)
        doNothing().when(operationLogService).record(
                anyString(), anyString(), any(), anyString(),
                anyString(), anyString(), anyString(),
                (int) anyLong(), any(HttpServletRequest.class), any(), anyString());

        R<Void> result = globalExceptionAdvice.handleRuntimeException(ex, request);

        assertNotNull(result);
        assertEquals(500, result.getCode());
        // 关键断言: 返回给客户端的消息不应包含原始异常信息
        assertNotNull(result.getMsg());
        assertFalse(result.getMsg().contains(sensitiveMessage),
                "不应向客户端泄露 e.getMessage(): " + result.getMsg());
        assertFalse(result.getMsg().contains("SELECT"),
                "不应向客户端泄露 SQL 语句: " + result.getMsg());
        assertFalse(result.getMsg().contains("admin123"),
                "不应向客户端泄露密码: " + result.getMsg());
    }

    @Test
    @DisplayName("handleRuntimeException 返回通用提示语")
    void handleRuntimeException_ReturnsGenericPrompt() {
        RuntimeException ex = new RuntimeException("NullPointerException at com.internal.Class");
        HttpServletRequest request = new MockHttpServletRequest();

        doNothing().when(operationLogService).record(
                anyString(), anyString(), any(), anyString(),
                anyString(), anyString(), anyString(),
                (int) anyLong(), any(HttpServletRequest.class), any(), anyString());

        R<Void> result = globalExceptionAdvice.handleRuntimeException(ex, request);

        assertEquals(500, result.getCode());
        assertEquals("系统内部错误，请稍后重试", result.getMsg());
    }

    @Test
    @DisplayName("handleRuntimeException 即使 getMessage 为 null 也能正常处理")
    void handleRuntimeException_NullMessage_HandledGracefully() {
        RuntimeException ex = new RuntimeException();
        HttpServletRequest request = new MockHttpServletRequest();

        doNothing().when(operationLogService).record(
                anyString(), anyString(), any(), anyString(),
                anyString(), anyString(), anyString(),
                (int) anyLong(), any(HttpServletRequest.class), any(), anyString());

        R<Void> result = globalExceptionAdvice.handleRuntimeException(ex, request);

        assertEquals(500, result.getCode());
        assertNotNull(result.getMsg());
    }
}
