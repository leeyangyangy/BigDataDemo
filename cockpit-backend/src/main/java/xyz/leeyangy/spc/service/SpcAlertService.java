package xyz.leeyangy.spc.service;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.IService;
import xyz.leeyangy.spc.entity.SpcAlert;
import xyz.leeyangy.spc.vo.AlertHandleResultVO;

import java.time.LocalDateTime;

/**
 * SPC 报警 Service 接口
 */
public interface SpcAlertService extends IService<SpcAlert> {

    Page<SpcAlert> pageByCondition(Page<SpcAlert> page,
                                    Long paramVersionId, Long paramId,
                                    String status,
                                    LocalDateTime startTime, LocalDateTime endTime);

    Page<SpcAlert> pageByCondition(Page<SpcAlert> page,
                                    Long paramVersionId, String batchId,
                                    String status, String alertType,
                                    Integer alertLevel);

    boolean confirmAlert(Long id, Long userId);

    AlertHandleResultVO handleAlertWithResult(Long id, String handleResult, String handleRemark, Long userId);

    boolean handleAlert(Long id, String handleResult, String handleRemark, Long userId);
}
