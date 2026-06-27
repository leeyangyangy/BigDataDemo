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

    @OperationLog(module = "SPC_DATA", action = "IMPORT_EXCEL", targetType = "SpcData",
            content = "'导入数据: file=' + #file.originalFilename + ' paramVersionId=' + #paramVersionId + ' total=' + #result.data['totalCount'] + ' success=' + #result.data['successCount']",
            resultExpression = "#result.data['successCount'] == #result.data['totalCount'] ? 'SUCCESS' : 'PARTIAL'")
    @PostMapping("/import")
    public R<Map<String, Object>> importExcel(@RequestParam("file") MultipartFile file,
                                              @RequestParam Long paramVersionId,
                                              @RequestAttribute Long userId) {
        Map<String, Object> result = spcDataImportExportService.importFromExcel(file, paramVersionId, userId);
        return R.ok(result);
    }

    @OperationLog(module = "SPC_DATA", action = "EXPORT_EXCEL", targetType = "SpcData",
            content = "'导出数据: paramVersionId=' + #paramVersionId + (#startTime != null ? ' start=' + #startTime : '') + (#endTime != null ? ' end=' + #endTime : '')")
    @GetMapping("/export/report")
    public void exportExcel(
            @RequestParam Long paramVersionId,
            @RequestParam(required = false) @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") LocalDateTime startTime,
            @RequestParam(required = false) @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") LocalDateTime endTime,
            HttpServletResponse response) {
        try {
            spcDataImportExportService.exportToExcel(paramVersionId, startTime, endTime, response);
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
}
