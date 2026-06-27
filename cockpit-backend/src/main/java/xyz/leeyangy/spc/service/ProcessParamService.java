package xyz.leeyangy.spc.service;

import com.baomidou.mybatisplus.extension.service.IService;
import lombok.Data;
import xyz.leeyangy.spc.entity.ProcessParam;
import xyz.leeyangy.spc.vo.ParamSimpleVO;

import java.util.List;

/**
 * 工序参数绑定 Service 接口
 */
public interface ProcessParamService extends IService<ProcessParam> {

    List<Long> getParamIdsByProcessId(Long processId);

    List<ParamSimpleVO> getProcessParams(Long processId);

    boolean bindParams(Long processId, List<BindItem> items);

    boolean bindParam(Long processId, Long paramId);

    boolean unbindParam(Long processId, Long paramId);

    /**
     * 参数绑定项
     */
    @Data
    class BindItem {
        private Long paramId;
    }
}
