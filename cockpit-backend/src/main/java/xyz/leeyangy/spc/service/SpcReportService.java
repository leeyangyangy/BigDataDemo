package xyz.leeyangy.spc.service;

import xyz.leeyangy.spc.vo.SpcReportData;

import javax.servlet.http.HttpServletResponse;
import java.time.LocalDateTime;

/**
 * SPC 分析报告 Service
 */
public interface SpcReportService {

    /**
     * 生成 SPC 分析报告 PDF 并写入 response
     *
     * @param paramId      工艺参数ID
     * @param productId    产品ID
     * @param equipmentId  设备ID(可空)
     * @param limit        样本数上限(可空)
     * @param startTime    起始时间(可空)
     * @param endTime      结束时间(可空)
     * @param charts       前端 ECharts 截图(base64)
     * @param response     HTTP 响应
     */
    void generatePdfReport(Long paramId, Long productId, Long equipmentId, Integer limit,
                           LocalDateTime startTime, LocalDateTime endTime,
                           SpcReportData.Charts charts,
                           HttpServletResponse response);
}
