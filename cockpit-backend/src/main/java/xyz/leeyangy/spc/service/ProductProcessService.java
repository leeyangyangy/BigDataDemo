package xyz.leeyangy.spc.service;

import com.baomidou.mybatisplus.extension.service.IService;
import lombok.Data;
import xyz.leeyangy.spc.entity.ProductProcess;
import xyz.leeyangy.spc.vo.ProcessBindingVO;

import java.util.List;

/**
 * 产品工序绑定 Service 接口
 */
public interface ProductProcessService extends IService<ProductProcess> {

    List<Long> getProcessIdsByProductId(Long productId);

    List<ProductProcess> getBindingsByProductId(Long productId);

    List<ProcessBindingVO> getProductProcessBindings(Long productId);

    boolean bindProcesses(Long productId, List<BindItem> items);

    boolean bindProcess(Long productId, Long processId);

    boolean unbindProcess(Long productId, Long processId);

    /**
     * 工序绑定项
     */
    @Data
    class BindItem {
        private Long processId;
    }
}
