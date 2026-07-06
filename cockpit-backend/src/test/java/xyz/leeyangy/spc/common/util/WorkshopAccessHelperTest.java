package xyz.leeyangy.spc.common.util;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import xyz.leeyangy.spc.common.exception.BusinessStateException;
import xyz.leeyangy.spc.service.SysUserWorkshopService;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * 车间访问权限工具单元测试。
 *
 * <p>重点验证 P0 修复: IDOR 防护 - 用户只能访问已绑定的车间数据。
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("车间访问权限工具测试 (IDOR 防护)")
class WorkshopAccessHelperTest {

    @Mock
    private SysUserWorkshopService sysUserWorkshopService;

    @InjectMocks
    private WorkshopAccessHelper workshopAccessHelper;

    @Test
    @DisplayName("有车间+测试站绑定时返回合并集合")
    void getAccessibleWorkshopIds_WithBindings_ReturnsMergedSet() {
        when(sysUserWorkshopService.getWorkshopIds(1L))
                .thenReturn(Arrays.asList(10L, 20L));
        when(sysUserWorkshopService.getTestStationIds(1L))
                .thenReturn(Arrays.asList(30L, 40L));

        Set<Long> result = workshopAccessHelper.getAccessibleWorkshopIds(1L);

        assertNotNull(result);
        assertEquals(4, result.size());
        assertTrue(result.containsAll(Arrays.asList(10L, 20L, 30L, 40L)));
    }

    @Test
    @DisplayName("仅有车间绑定时返回车间集合")
    void getAccessibleWorkshopIds_OnlyWorkshops_ReturnsWorkshopSet() {
        when(sysUserWorkshopService.getWorkshopIds(1L))
                .thenReturn(Arrays.asList(10L, 20L));
        when(sysUserWorkshopService.getTestStationIds(1L))
                .thenReturn(Collections.emptyList());

        Set<Long> result = workshopAccessHelper.getAccessibleWorkshopIds(1L);

        assertEquals(2, result.size());
        assertTrue(result.containsAll(Arrays.asList(10L, 20L)));
    }

    @Test
    @DisplayName("无任何绑定时返回空集 (查询应返回空结果, 而非全部数据)")
    void getAccessibleWorkshopIds_NoBindings_ReturnsEmptySet() {
        when(sysUserWorkshopService.getWorkshopIds(1L))
                .thenReturn(Collections.emptyList());
        when(sysUserWorkshopService.getTestStationIds(1L))
                .thenReturn(Collections.emptyList());

        Set<Long> result = workshopAccessHelper.getAccessibleWorkshopIds(1L);

        assertNotNull(result, "空集不为 null (null=不限制, 空集=无权访问)");
        assertTrue(result.isEmpty());
    }

    @Test
    @DisplayName("null userId 返回空集")
    void getAccessibleWorkshopIds_NullUserId_ReturnsEmptySet() {
        Set<Long> result = workshopAccessHelper.getAccessibleWorkshopIds(null);

        assertNotNull(result);
        assertTrue(result.isEmpty());
        verifyNoInteractions(sysUserWorkshopService);
    }

    @Test
    @DisplayName("车间列表返回 null 时安全处理 (返回空集)")
    void getAccessibleWorkshopIds_NullLists_ReturnsEmptySet() {
        when(sysUserWorkshopService.getWorkshopIds(1L)).thenReturn(null);
        when(sysUserWorkshopService.getTestStationIds(1L)).thenReturn(null);

        Set<Long> result = workshopAccessHelper.getAccessibleWorkshopIds(1L);

        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    @DisplayName("有权访问车间时不抛异常")
    void checkWorkshopAccess_HasAccess_NoException() {
        when(sysUserWorkshopService.getWorkshopIds(1L))
                .thenReturn(Arrays.asList(10L, 20L));
        when(sysUserWorkshopService.getTestStationIds(1L))
                .thenReturn(Collections.emptyList());

        assertDoesNotThrow(() -> workshopAccessHelper.checkWorkshopAccess(1L, 10L));
        assertDoesNotThrow(() -> workshopAccessHelper.checkWorkshopAccess(1L, 20L));
    }

    @Test
    @DisplayName("无权访问车间时抛出 BusinessStateException")
    void checkWorkshopAccess_NoAccess_ThrowsException() {
        when(sysUserWorkshopService.getWorkshopIds(1L))
                .thenReturn(Arrays.asList(10L, 20L));
        when(sysUserWorkshopService.getTestStationIds(1L))
                .thenReturn(Collections.emptyList());

        BusinessStateException ex = assertThrows(
                BusinessStateException.class,
                () -> workshopAccessHelper.checkWorkshopAccess(1L, 999L)
        );

        assertTrue(ex.getMessage().contains("无权访问"));
    }

    @Test
    @DisplayName("null workshopId 不校验 (直接放行)")
    void checkWorkshopAccess_NullWorkshopId_NoException() {
        assertDoesNotThrow(() -> workshopAccessHelper.checkWorkshopAccess(1L, null));
        verifyNoInteractions(sysUserWorkshopService);
    }
}
