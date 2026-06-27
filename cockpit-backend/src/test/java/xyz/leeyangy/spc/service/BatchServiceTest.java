package xyz.leeyangy.spc.service;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import xyz.leeyangy.spc.entity.Batch;
import xyz.leeyangy.spc.mapper.BatchMapper;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("批次服务单元测试")
class BatchServiceTest {

    @Mock
    private BatchMapper batchMapper;

    @InjectMocks
    private BatchService batchService;

    private Batch testBatch;

    @BeforeEach
    void setUp() {
        testBatch = new Batch();
        testBatch.setId(1L);
        testBatch.setBatchCode("BATCH-001");
        testBatch.setProductId(100L);
        testBatch.setProcessId(200L);
    }

    @Test
    @DisplayName("创建批次 - 成功")
    void createBatch_Success() {
        when(batchMapper.insert(any(Batch.class))).thenReturn(1);

        Batch result = batchService.createBatch(testBatch);

        assertNotNull(result);
        assertEquals(Integer.valueOf(1), result.getStatus());
        verify(batchMapper, times(1)).insert(any(Batch.class));
    }

    @Test
    @DisplayName("更新批次 - 成功")
    void updateBatch_Success() {
        when(batchMapper.updateById(any(Batch.class))).thenReturn(1);

        Batch updateData = new Batch();
        updateData.setBatchCode("BATCH-001-UPDATED");

        Batch result = batchService.updateBatch(1L, updateData);

        assertNotNull(result);
        assertEquals(Long.valueOf(1L), result.getId());
        assertEquals("BATCH-001-UPDATED", result.getBatchCode());
        verify(batchMapper, times(1)).updateById(any(Batch.class));
    }

    @Test
    @DisplayName("删除批次 - 批次不存在时抛出异常")
    void deleteBatch_NotExist_ThrowsException() {
        when(batchMapper.selectById(999L)).thenReturn(null);

        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            batchService.deleteBatch(999L);
        });

        assertTrue(exception.getMessage().contains("不存在"));
    }

    @Test
    @DisplayName("删除批次 - 成功")
    void deleteBatch_Success() {
        when(batchMapper.selectById(1L)).thenReturn(testBatch);
        when(batchMapper.deleteById(1L)).thenReturn(1);

        boolean result = batchService.deleteBatch(1L);

        assertTrue(result);
        verify(batchMapper, times(1)).deleteById(1L);
    }
}
