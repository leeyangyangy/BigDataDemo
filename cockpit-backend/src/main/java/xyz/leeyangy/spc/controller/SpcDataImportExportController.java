package xyz.leeyangy.spc.controller;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import xyz.leeyangy.spc.common.R;
import xyz.leeyangy.spc.entity.*;
import xyz.leeyangy.spc.entity.Process;
import xyz.leeyangy.spc.service.*;

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

@RestController
@RequestMapping("/api/spc/data")
@RequiredArgsConstructor
@Slf4j
public class SpcDataImportExportController {

    private final SpcDataService spcDataService;
    private final ParamVersionService paramVersionService;
    private final ProductService productService;
    private final ProcessService processService;
    private final ParamService paramService;
    private final EquipmentService equipmentService;
    private final SpcStatService spcStatService;
    private final SpcAlertService spcAlertService;

    @PostMapping("/import")
    public R<Map<String, Object>> importData(
            @RequestParam("file") MultipartFile file,
            @RequestParam Long productId,
            @RequestParam(required = false) Long processId,
            @RequestParam(required = false) Long equipmentId) {

        if (file.isEmpty()) {
            return R.fail("文件不能为空");
        }

        String originalFilename = file.getOriginalFilename();
        if (originalFilename == null || (!originalFilename.endsWith(".csv") && !originalFilename.endsWith(".xlsx") && !originalFilename.endsWith(".xls"))) {
            return R.fail("仅支持 CSV、XLS、XLSX 格式");
        }

        try {
            List<Map<String, String>> parsedData = parseFile(file);

            if (parsedData.isEmpty()) {
                return R.fail("文件中没有有效数据");
            }

            int successCount = 0;
            int failCount = 0;
            List<String> errors = new ArrayList<>();

            for (int i = 0; i < parsedData.size(); i++) {
                Map<String, String> row = parsedData.get(i);
                try {
                    SpcData data = new SpcData();
                    data.setProductId(productId);
                    data.setProcessId(processId);
                    data.setEquipmentId(equipmentId);

                    String paramName = row.get("paramName") != null ? row.get("paramName") : row.get("工艺参数");
                    if (paramName == null || paramName.trim().isEmpty()) {
                        throw new IllegalArgumentException("第" + (i + 2) + "行: 工艺参数名称为空");
                    }

                    LambdaQueryWrapper<Param> paramWrapper = new LambdaQueryWrapper<>();
                    paramWrapper.eq(Param::getParamName, paramName.trim());
                    Param param = paramService.getOne(paramWrapper);
                    if (param == null) {
                        throw new IllegalArgumentException("第" + (i + 2) + "行: 未找到工艺参数 [" + paramName + "]");
                    }
                    data.setParamId(param.getId());

                    String valueStr = row.get("value") != null ? row.get("value") : row.get("测量值");
                    if (valueStr == null || valueStr.trim().isEmpty()) {
                        throw new IllegalArgumentException("第" + (i + 2) + "行: 测量值为空");
                    }
                    data.setMeasuredValue(new BigDecimal(valueStr.trim()));

                    String batchId = row.get("batchId") != null ? row.get("batchId") : row.get("批次号");
                    data.setBatchId(batchId != null ? batchId.trim() : null);

                    String fillTimeStr = row.get("fillTime") != null ? row.get("fillTime") : row.get("时间");
                    if (fillTimeStr != null && !fillTimeStr.trim().isEmpty()) {
                        data.setCollectTime(LocalDateTime.parse(fillTimeStr.trim(), DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
                    } else {
                        data.setCollectTime(LocalDateTime.now());
                    }

                    spcDataService.save(data);
                    successCount++;
                } catch (Exception e) {
                    failCount++;
                    errors.add("第" + (i + 2) + "行: " + e.getMessage());
                    log.warn("导入数据失败: {}", e.getMessage());
                }
            }

            Map<String, Object> result = new LinkedHashMap<>();
            result.put("totalCount", parsedData.size());
            result.put("successCount", successCount);
            result.put("failCount", failCount);
            result.put("errors", errors);

            return R.ok("导入完成: 成功" + successCount + "条, 失败" + failCount + "条", result);

        } catch (Exception e) {
            log.error("解析文件失败", e);
            return R.fail("解析文件失败: " + e.getMessage());
        }
    }

    @GetMapping("/template/download")
    public ResponseEntity<byte[]> downloadTemplate() {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            StringBuilder sb = new StringBuilder();

            sb.append("\uFEFF");
            sb.append("工艺参数,测量值,批次号,时间\n");
            sb.append("示例参数1,10.5,BATCH001,2026-01-01 08:00:00\n");
            sb.append("示例参数1,10.8,BATCH001,2026-01-02 09:30:00\n");
            sb.append("示例参数2,25.3,BATCH002,2026-01-03 14:15:00\n");

            out.write(sb.toString().getBytes(StandardCharsets.UTF_8));
            byte[] data = out.toByteArray();

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.parseMediaType("text/csv"));
            headers.setContentDispositionFormData("attachment",
                    URLEncoder.encode("SPC数据导入模板.csv", StandardCharsets.UTF_8).replaceAll("\\+", "%20"));

            return ResponseEntity.ok()
                    .headers(headers)
                    .body(data);
        } catch (Exception e) {
            log.error("生成模板失败", e);
            return ResponseEntity.internalServerError().build();
        }
    }

    @GetMapping("/export/report")
    public ResponseEntity<byte[]> exportReport(
            @RequestParam Long paramId,
            @RequestParam Long productId,
            @RequestParam(required = false) Long equipmentId,
            @RequestParam(required = false) String startTime,
            @RequestParam(required = false) String endTime,
            @RequestParam(defaultValue = "100") Integer limit) {

        try {
            ParamVersion version = paramVersionService.getCurrentVersion(paramId, productId);
            if (version == null) {
                return ResponseEntity.ok()
                        .contentType(MediaType.APPLICATION_OCTET_STREAM)
                        .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=error.txt")
                        .body("未找到该参数的版本配置".getBytes(StandardCharsets.UTF_8));
            }

            LocalDateTime start = startTime != null ? LocalDateTime.parse(startTime, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")) : null;
            LocalDateTime end = endTime != null ? LocalDateTime.parse(endTime, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")) : null;

            List<SpcData> dataList = spcDataService.listRecentData(version.getId(), limit, start, end);

            if (equipmentId != null) {
                dataList.removeIf(d -> !equipmentId.equals(d.getEquipmentId()));
            }

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            StringBuilder sb = new StringBuilder();
            sb.append("\uFEFF");

            sb.append("===== SPC分析报告 =====\n\n");
            sb.append("生成时间: ").append(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))).append("\n\n");

            sb.append("--- 基本信息 ---\n");
            Product product = productService.getById(productId);
            Param param = paramService.getById(paramId);
            xyz.leeyangy.spc.entity.Process process = param != null ? processService.getById(param.getProcessId()) : null;

            sb.append("产品: ").append(product != null ? product.getProductName() : "-").append("\n");
            sb.append("工序: ").append(process != null ? process.getProcessName() : "-").append("\n");
            sb.append("工艺参数: ").append(param != null ? param.getParamName() : "-").append("\n");
            sb.append("版本号: V").append(version.getVersionNo()).append("\n");
            sb.append("数据点数: ").append(dataList.size()).append("\n\n");

            sb.append("--- 规格限 ---\n");
            sb.append("USL: ").append(version.getUsl() != null ? version.getUsl().toString() : "-").append("\n");
            sb.append("Target: ").append(version.getTarget() != null ? version.getTarget().toString() : "-").append("\n");
            sb.append("LSL: ").append(version.getLsl() != null ? version.getLsl().toString() : "-").append("\n\n");

            sb.append("--- 控制限 ---\n");
            sb.append("UCL: ").append(version.getUcl() != null ? version.getUcl().toString() : "-").append("\n");
            sb.append("CL: ").append(version.getCl() != null ? version.getCl().toString() : "-").append("\n");
            sb.append("LCL: ").append(version.getLcl() != null ? version.getLcl().toString() : "-").append("\n\n");

            SpcStatResult statResult = spcStatService.getLatestStat(version.getId(), null);
            if (statResult != null) {
                sb.append("--- 统计分析 ---\n");
                sb.append("样本数: ").append(statResult.getSampleCount()).append("\n");
                sb.append("均值(X̄): ").append(statResult.getMeanValue() != null ? statResult.getMeanValue().toString() : "-").append("\n");
                sb.append("标准差(σ): ").append(statResult.getStdDev() != null ? statResult.getStdDev().toString() : "-").append("\n");
                sb.append("极差(R): ").append(statResult.getRangeValue() != null ? statResult.getRangeValue().toString() : "-").append("\n\n");
                
                sb.append("--- 过程能力指数 ---\n");
                sb.append("Cp: ").append(formatCpIndex(statResult.getCp())).append("\n");
                sb.append("Cpk: ").append(formatCpIndex(statResult.getCpk())).append("\n");
                sb.append("Pp: ").append(formatCpIndex(statResult.getPp())).append("\n");
                sb.append("Ppk: ").append(formatCpIndex(statResult.getPpk())).append("\n");
                sb.append("合格率: ").append(statResult.getPassRate() != null ? statResult.getPassRate().multiply(new BigDecimal("100")).setScale(2, java.math.RoundingMode.HALF_UP) + "%" : "-").append("\n");
                sb.append("合格/不合格: ").append(statResult.getPassCount()).append("/").append(statResult.getFailCount()).append("\n\n");
                
                if (statResult.getIsNormal() != null) {
                    sb.append("--- 正态性检验 ---\n");
                    sb.append("W统计量: ").append(statResult.getNormalityW() != null ? statResult.getNormalityW().toString() : "-").append("\n");
                    sb.append("P值: ").append(statResult.getNormalityPValue() != null ? statResult.getNormalityPValue().toString() : "-").append("\n");
                    sb.append("是否正态: ").append(statResult.getIsNormal() ? "是" : "否").append("\n\n");
                }
            }

            List<SpcAlert> alertList = spcAlertService.list(new LambdaQueryWrapper<SpcAlert>()
                    .eq(SpcAlert::getParamVersionId, version.getId())
                    .eq(SpcAlert::getDeleted, 0)
                    .orderByDesc(SpcAlert::getAlertTime)
                    .last("LIMIT 50"));
            
            if (!alertList.isEmpty()) {
                sb.append("--- 异常记录 (最近50条) ---\n");
                sb.append("序号,时间,规则编号,规则描述,测量值,状态\n");
                for (int i = 0; i < alertList.size(); i++) {
                    SpcAlert alert = alertList.get(i);
                    sb.append(i + 1).append(",");
                    sb.append(alert.getAlertTime() != null ? alert.getAlertTime().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")) : "-").append(",");
                    sb.append(alert.getRuleNumber() != null ? "Rule" + alert.getRuleNumber() : "-").append(",");
                    sb.append(alert.getMessage() != null ? escapeCsv(alert.getMessage()) : (alert.getRuleName() != null ? escapeCsv(alert.getRuleName()) : "-")).append(",");
                    sb.append(alert.getMeasuredValue() != null ? alert.getMeasuredValue().toString() : "-").append(",");
                    sb.append("1".equals(alert.getStatus()) ? "已处理" : "待处理").append("\n");
                }
                sb.append("\n");
            }

            sb.append("--- 数据明细 ---\n");
            sb.append("序号,时间,测量值,设备ID,批次号,是否异常,区域\n");

            for (int i = 0; i < dataList.size(); i++) {
                SpcData d = dataList.get(i);
                sb.append(i + 1).append(",");
                sb.append(d.getCollectTime() != null ? d.getCollectTime().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")) : "-").append(",");
                sb.append(d.getMeasuredValue() != null ? d.getMeasuredValue().toString() : "-").append(",");
                sb.append(d.getEquipmentId() != null ? d.getEquipmentId().toString() : "-").append(",");
                sb.append(d.getBatchId() != null ? d.getBatchId() : "-").append(",");
                sb.append(d.getIsOoc() != null && d.getIsOoc() == 1 ? "是" : "否").append(",");
                sb.append(d.getZone() != null ? d.getZone().toString() : "-").append("\n");
            }

            out.write(sb.toString().getBytes(StandardCharsets.UTF_8));
            byte[] data = out.toByteArray();

            String filename = URLEncoder.encode(
                    "SPC报告_" + (param != null ? param.getParamName() : "report") + "_" +
                            LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")) + ".csv",
                    StandardCharsets.UTF_8).replaceAll("\\+", "%20");

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.parseMediaType("text/csv"));
            headers.setContentDispositionFormData("attachment", filename);

            return ResponseEntity.ok()
                    .headers(headers)
                    .body(data);

        } catch (Exception e) {
            log.error("生成报告失败", e);
            return ResponseEntity.internalServerError().build();
        }
    }

    private List<Map<String, String>> parseFile(MultipartFile file) throws Exception {
        List<Map<String, String>> result = new ArrayList<>();
        String filename = file.getOriginalFilename();

        if (filename != null && filename.endsWith(".csv")) {
            String content = new String(file.getBytes(), StandardCharsets.UTF_8);
            String[] lines = content.split("\n");

            if (lines.length < 2) return result;

            String[] headers = lines[0].replace("\uFEFF", "").split(",");

            for (int i = 1; i < lines.length; i++) {
                String line = lines[i].trim();
                if (line.isEmpty()) continue;

                String[] values = line.split(",");
                Map<String, String> row = new LinkedHashMap<>();

                for (int j = 0; j < headers.length && j < values.length; j++) {
                    row.put(headers[j].trim(), values[j].trim());
                }

                result.add(row);
            }
        } else {
            throw new UnsupportedOperationException("暂不支持 Excel 格式，请使用 CSV 格式");
        }

        return result;
    }

    private String formatCpIndex(BigDecimal value) {
        if (value == null) return "-";
        if (value.compareTo(new BigDecimal("1.33")) >= 0) return value.setScale(3, java.math.RoundingMode.HALF_UP).toString() + " (良好)";
        if (value.compareTo(new BigDecimal("1.0")) >= 0) return value.setScale(3, java.math.RoundingMode.HALF_UP).toString() + " (一般)";
        if (value.compareTo(new BigDecimal("0.67")) >= 0) return value.setScale(3, java.math.RoundingMode.HALF_UP).toString() + " (不足)";
        return value.setScale(3, java.math.RoundingMode.HALF_UP).toString() + " (严重不足)";
    }

    private String escapeCsv(String value) {
        if (value == null) return "-";
        if (value.contains(",") || value.contains("\"") || value.contains("\n")) {
            return "\"" + value.replace("\"", "\"\"") + "\"";
        }
        return value;
    }
}