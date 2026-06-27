package xyz.leeyangy.spc.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import xyz.leeyangy.spc.entity.ProductProcess;
import xyz.leeyangy.spc.mapper.ProductProcessMapper;
import xyz.leeyangy.spc.vo.ProcessBindingVO;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("产品工序关联服务单元测试")
class ProductProcessServiceTest {

    @Mock
    private ProductProcessMapper productProcessMapper;

    @InjectMocks
    private ProductProcessService productProcessService;

    private ProductProcess testBinding1;
    private ProductProcess testBinding2;

    @BeforeEach
    void setUp() {
        testBinding1 = new ProductProcess();
        testBinding1.setId(1L);
        testBinding1.setProductId(100L);
        testBinding1.setProcessId(1L);
        testBinding1.setSortOrder(0);

        testBinding2 = new ProductProcess();
        testBinding2.setId(2L);
        testBinding2.setProductId(100L);
        testBinding2.setProcessId(2L);
        testBinding2.setSortOrder(1);
    }

    @Test
    @DisplayName("获取产品关联的工序ID列表 - 成功")
    void getProcessIdsByProductId_Success() {
        when(productProcessMapper.selectList(any())).thenReturn(Arrays.asList(testBinding1, testBinding2));

        List<Long> processIds = productProcessService.getProcessIdsByProductId(100L);

        assertNotNull(processIds);
        assertEquals(2, processIds.size());
        assertTrue(processIds.contains(1L));
        assertTrue(processIds.contains(2L));
    }

    @Test
    @DisplayName("获取产品工序绑定信息 - 成功")
    void getProductProcessBindings_Success() {
        when(productProcessMapper.selectList(any())).thenReturn(Arrays.asList(testBinding1, testBinding2));

        List<ProcessBindingVO> bindings = productProcessService.getProductProcessBindings(100L);

        assertNotNull(bindings);
        assertEquals(2, bindings.size());
        assertEquals(Long.valueOf(1L), bindings.get(0).getProcessId());
        assertEquals(Integer.valueOf(0), bindings.get(0).getSortOrder());
    }

    @Test
    @DisplayName("获取产品工序绑定信息 - 无数据时返回空列表")
    void getProductProcessBindings_EmptyResult() {
        when(productProcessMapper.selectList(any())).thenReturn(Collections.emptyList());

        List<ProcessBindingVO> bindings = productProcessService.getProductProcessBindings(999L);

        assertNotNull(bindings);
        assertTrue(bindings.isEmpty());
    }

    @Test
    @DisplayName("绑定工序到产品 - 成功")
    void bindProcesses_Success() {
        doNothing().when(productProcessMapper).physicalDeleteByProductId(anyLong());
        when(productProcessMapper.insert(any(ProductProcess.class))).thenReturn(1);

        List<ProductProcessService.BindItem> items = Arrays.asList(
                createBindItem(1L),
                createBindItem(2L)
        );

        boolean result = productProcessService.bindProcesses(100L, items);

        assertTrue(result);
        verify(productProcessMapper, times(1)).physicalDeleteByProductId(100L);
        verify(productProcessMapper, times(2)).insert(any(ProductProcess.class));
    }

    @Test
    @DisplayName("解绑产品工序 - 成功")
    void unbindProcess_Success() {
        when(productProcessMapper.physicalDelete(anyLong(), anyLong())).thenReturn(1);

        boolean result = productProcessService.unbindProcess(100L, 1L);

        assertTrue(result);
        verify(productProcessMapper, times(1)).physicalDelete(100L, 1L);
    }

    private ProductProcessService.BindItem createBindItem(Long processId) {
        ProductProcessService.BindItem item = new ProductProcessService.BindItem();
        item.setProcessId(processId);
        return item;
    }
}
