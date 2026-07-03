package xyz.leeyangy.spc.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import xyz.leeyangy.spc.common.R;
import xyz.leeyangy.spc.common.annotation.OperationLog;
import xyz.leeyangy.spc.common.exception.BusinessException;
import xyz.leeyangy.spc.service.SpcDataImportExportService;
import xyz.leeyangy.spc.service.SpcReportService;
import xyz.leeyangy.spc.vo.SpcReportData;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/spc/data")
@RequiredArgsConstructor
public class SpcDataImportExportController {

    private final SpcDataImportExportService spcDataImportExportService;
    private final SpcReportService spcReportService;

    @OperationLog(module = "SPC_DATA", action = "IMPORT_EXCEL", targetType = "SpcData",
            content = "'导入数据: file=' + #file.originalFilename + ' paramId=' + #paramId + ' productId=' + #productId + ' total=' + #result.data['totalCount'] + ' success=' + #result.data['successCount']",
            resultExpression = "#result.data['successCount'] == #result.data['totalCount'] ? 'SUCCESS' : 'PARTIAL'")
    @PostMapping("/import")
    public R<Map<String, Object>> importExcel(@RequestParam("file") MultipartFile file,
                                              @RequestParam Long paramId,
                                              @RequestParam Long productId,
                                              @RequestParam(required = false) Long processId,
                                              @RequestParam(required = false) Long equipmentId,
                                              @RequestAttribute Long userId,
                                              @RequestAttribute String role) {
        Map<String, Object> result = spcDataImportExportService.importFromExcel(file, paramId, productId, processId, equipmentId, userId, role);
        return R.ok(result);
    }

    @OperationLog(module = "SPC_DATA", action = "EXPORT_EXCEL", targetType = "SpcData",
            content = "'导出数据: paramId=' + #paramId + ' productId=' + #productId + (#equipmentId != null ? ' equipmentId=' + #equipmentId : '') + (#limit != null ? ' limit=' + #limit : '') + (#startTime != null ? ' start=' + #startTime : '') + (#endTime != null ? ' end=' + #endTime : '')")
    @GetMapping("/export/report")
    public void exportExcel(
            @RequestParam Long paramId,
            @RequestParam Long productId,
            @RequestParam(required = false) Long equipmentId,
            @RequestParam(required = false) Integer limit,
            @RequestParam(required = false) @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") LocalDateTime startTime,
            @RequestParam(required = false) @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") LocalDateTime endTime,
            HttpServletResponse response) {
        try {
            spcDataImportExportService.exportToExcel(paramId, productId, equipmentId, limit, startTime, endTime, response);
        } catch (BusinessException e) {
            throw e;
        } catch (Exception e) {
            log.error("[Export] 导出失败: {}", e.getMessage(), e);
            throw new BusinessException("导出失败: " + e.getMessage());
        }
    }

    @GetMapping("/template")
    public void downloadTemplate(HttpServletResponse response) throws IOException {
        spcDataImportExportService.downloadTemplate(response);
    }

    /**
     * 生成 SPC 分析报告 PDF(对标 SPC 统计过程控制分析报告)
     *
     * <p>前端用 ECharts 渲染图表后截图,以 multipart/form-data 上送 6 张 base64 图,
     * 后端组装基本信息/统计/控制限/过程能力/异常分析 + 图表,生成 PDF。</p>
     */
    @OperationLog(module = "SPC_DATA", action = "EXPORT_PDF_REPORT", targetType = "SpcData",
            content = "'导出SPC报告: paramId=' + #paramId + ' productId=' + #productId + (#equipmentId != null ? ' equipmentId=' + #equipmentId : '') + (#limit != null ? ' limit=' + #limit : '')")
    @PostMapping("/export/pdf-report")
    public void exportPdfReport(
            @RequestParam Long paramId,
            @RequestParam Long productId,
            @RequestParam(required = false) Long equipmentId,
            @RequestParam(required = false) Integer limit,
            @RequestParam(required = false) @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") LocalDateTime startTime,
            @RequestParam(required = false) @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") LocalDateTime endTime,
            @RequestParam(required = false) String controlChartImr,
            @RequestParam(required = false) String controlChartXbar,
            @RequestParam(required = false) String controlChartR,
            @RequestParam(required = false) String capabilityHistogram,
            @RequestParam(required = false) String normalProbabilityPlot,
            @RequestParam(required = false) String trendChart,
            HttpServletResponse response) {
        try {
            SpcReportData.Charts charts = new SpcReportData.Charts();
            charts.setControlChartImr(controlChartImr);
            charts.setControlChartXbar(controlChartXbar);
            charts.setControlChartR(controlChartR);
            charts.setCapabilityHistogram(capabilityHistogram);
            charts.setNormalProbabilityPlot(normalProbabilityPlot);
            charts.setTrendChart(trendChart);

            spcReportService.generatePdfReport(paramId, productId, equipmentId, limit,
                    startTime, endTime, charts, response);
        } catch (BusinessException e) {
            throw e;
        } catch (Exception e) {
            log.error("[Report] SPC 报告生成失败: {}", e.getMessage(), e);
            throw new BusinessException("SPC 报告生成失败: " + e.getMessage());
        }
    }
}
