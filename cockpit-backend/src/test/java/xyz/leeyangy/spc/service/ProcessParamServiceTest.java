package xyz.leeyangy.spc.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import xyz.leeyangy.spc.entity.Param;
import xyz.leeyangy.spc.entity.ProcessParam;
import xyz.leeyangy.spc.mapper.ProcessParamMapper;
import xyz.leeyangy.spc.vo.ParamSimpleVO;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("工序参数关联服务单元测试")
class ProcessParamServiceTest {

    @Mock
    private ProcessParamMapper processParamMapper;

    @Mock
    private ParamService paramService;

    @InjectMocks
    private ProcessParamService processParamService;

    private ProcessParam testBinding1;
    private ProcessParam testBinding2;
    private Param testParam1;
    private Param testParam2;

    @BeforeEach
    void setUp() {
        testBinding1 = new ProcessParam();
        testBinding1.setId(1L);
        testBinding1.setProcessId(10L);
        testBinding1.setParamId(100L);
        testBinding1.setSortOrder(0);

        testBinding2 = new ProcessParam();
        testBinding2.setId(2L);
        testBinding2.setProcessId(10L);
        testBinding2.setParamId(200L);
        testBinding2.setSortOrder(1);

        testParam1 = new Param();
        testParam1.setId(100L);
        testParam1.setParamCode("PARAM-001");
        testParam1.setParamName("参数1");

        testParam2 = new Param();
        testParam2.setId(200L);
        testParam2.setParamCode("PARAM-002");
        testParam2.setParamName("参数2");
    }

    @Test
    @DisplayName("获取工序关联的参数ID列表 - 成功")
    void getParamIdsByProcessId_Success() {
        when(processParamMapper.selectList(any())).thenReturn(Arrays.asList(testBinding1, testBinding2));

        List<Long> paramIds = processParamService.getParamIdsByProcessId(10L);

        assertNotNull(paramIds);
        assertEquals(2, paramIds.size());
        assertTrue(paramIds.contains(100L));
        assertTrue(paramIds.contains(200L));
    }

    @Test
    @DisplayName("获取工序参数信息 - 成功")
    void getProcessParams_Success() {
        when(processParamMapper.selectList(any())).thenReturn(Arrays.asList(testBinding1, testBinding2));
        when(paramService.listByIds(anyList())).thenReturn(Arrays.asList(testParam1, testParam2));

        List<ParamSimpleVO> params = processParamService.getProcessParams(10L);

        assertNotNull(params);
        assertEquals(2, params.size());
        assertEquals(Long.valueOf(100L), params.get(0).getId());
        assertEquals("PARAM-001", params.get(0).getParamCode());
        assertEquals("参数1", params.get(0).getParamName());
    }

    @Test
    @DisplayName("获取工序参数信息 - 无数据时返回空列表")
    void getProcessParams_EmptyResult() {
        when(processParamMapper.selectList(any())).thenReturn(Collections.emptyList());

        List<ParamSimpleVO> params = processParamService.getProcessParams(999L);

        assertNotNull(params);
        assertTrue(params.isEmpty());
    }

    @Test
    @DisplayName("绑定参数到工序 - 成功")
    void bindParams_Success() {
        doNothing().when(processParamMapper).physicalDeleteByProcessId(anyLong());
        when(processParamMapper.insert(any(ProcessParam.class))).thenReturn(1);

        List<ProcessParamService.BindItem> items = Arrays.asList(
                createBindItem(100L),
                createBindItem(200L)
        );

        boolean result = processParamService.bindParams(10L, items);

        assertTrue(result);
        verify(processParamMapper, times(1)).physicalDeleteByProcessId(10L);
        verify(processParamMapper, times(2)).insert(any(ProcessParam.class));
    }

    @Test
    @DisplayName("解绑工序参数 - 成功")
    void unbindParam_Success() {
        when(processParamMapper.physicalDelete(anyLong(), anyLong())).thenReturn(1);

        boolean result = processParamService.unbindParam(10L, 100L);

        assertTrue(result);
        verify(processParamMapper, times(1)).physicalDelete(10L, 100L);
    }

    private ProcessParamService.BindItem createBindItem(Long paramId) {
        ProcessParamService.BindItem item = new ProcessParamService.BindItem();
        item.setParamId(paramId);
        return item;
    }
}
