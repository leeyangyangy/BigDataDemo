package xyz.leeyangy.spc.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import xyz.leeyangy.spc.common.StatusMsg;
import xyz.leeyangy.spc.common.exception.BusinessException;
import xyz.leeyangy.spc.common.exception.ResourceNotFoundException;
import xyz.leeyangy.spc.entity.SpcData;
import xyz.leeyangy.spc.entity.ParamVersion;
import xyz.leeyangy.spc.service.ParamVersionService;
import xyz.leeyangy.spc.service.SpcDataImportExportService;
import xyz.leeyangy.spc.service.SpcDataService;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Slf4j
@Service
@RequiredArgsConstructor
public class SpcDataImportExportServiceImpl implements SpcDataImportExportService {

    private final SpcDataService spcDataService;
    private final ParamVersionService paramVersionService;

    @Override
    public Map<String, Object> importFromExcel(MultipartFile file, Long paramId, Long productId,
                                               Long processId, Long equipmentId, Long userId, String role) {
        // 业务级参数校验（与前端校验对齐，防止绕过前端直接调接口）
        if (file == null || file.isEmpty()) {
            throw new BusinessException(StatusMsg.IMPORT_FILE_REQUIRED);
        }
        if (paramId == null || paramId <= 0) {
            throw new BusinessException(StatusMsg.IMPORT_PARAM_REQUIRED);
        }
        if (productId == null || productId <= 0) {
            throw new BusinessException(StatusMsg.IMPORT_PRODUCT_REQUIRED);
        }
        if (userId == null || userId <= 0) {
            throw new BusinessException(StatusMsg.IMPORT_USER_NOT_LOGIN);
        }
        if (role == null || role.trim().isEmpty()) {
            throw new BusinessException(StatusMsg.IMPORT_ROLE_MISSING);
        }

        Map<String, Object> result = new HashMap<>();
        List<SpcData> successList = new ArrayList<>();
        List<Map<String, String>> failList = new ArrayList<>();

        try (Workbook workbook = WorkbookFactory.create(file.getInputStream())) {
            Sheet sheet = workbook.getSheetAt(0);
            if (sheet == null || sheet.getPhysicalNumberOfRows() < 2) {
                result.put("error", "Excel文件格式错误或无数据");
                return result;
            }

            Row headerRow = sheet.getRow(0);
            int batchCol = -1, valueCol = -1, timeCol = -1, sampleSizeCol = -1;

            for (int i = 0; i < headerRow.getLastCellNum(); i++) {
                String cellValue = getCellValue(headerRow.getCell(i));
                if ("批次号".equals(cellValue)) batchCol = i;
                else if ("测量值".equals(cellValue)) valueCol = i;
                else if ("采集时间".equals(cellValue)) timeCol = i;
                else if ("样本量".equals(cellValue)) sampleSizeCol = i;
            }

            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

            for (int rowIndex = 1; rowIndex <= sheet.getLastRowNum(); rowIndex++) {
                Row row = sheet.getRow(rowIndex);
                if (row == null) continue;

                try {
                    SpcData data = new SpcData();
                    data.setParamId(paramId);
                    data.setProductId(productId);
                    data.setProcessId(processId);
                    data.setEquipmentId(equipmentId);
                    data.setCreatedBy(userId);

                    if (batchCol >= 0 && row.getCell(batchCol) != null) {
                        String batchVal = getCellValue(row.getCell(batchCol));
                        if (!batchVal.isEmpty()) {
                            data.setBatchId(batchVal);
                        }
                    }

                    if (valueCol >= 0 && row.getCell(valueCol) != null) {
                        String valStr = getCellValue(row.getCell(valueCol)).trim();
                        if (valStr.isEmpty()) {
                            throw new IllegalArgumentException("测量值为空");
                        }

                        try {
                            BigDecimal measuredValue = new BigDecimal(valStr);

                            // 验证数值合理性
                            if (measuredValue.compareTo(new BigDecimal("-999999999")) < 0 ||
                                measuredValue.compareTo(new BigDecimal("999999999")) > 0) {
                                throw new IllegalArgumentException("测量值超出合理范围: " + valStr);
                            }

                            data.setMeasuredValue(measuredValue);
                        } catch (NumberFormatException e) {
                            throw new IllegalArgumentException("测量值格式错误: '" + valStr + "'，请输入有效数字");
                        }
                    } else {
                        throw new IllegalArgumentException("缺少测量值列或测量值为空");
                    }

                    if (timeCol >= 0 && row.getCell(timeCol) != null) {
                        String timeStr = getCellValue(row.getCell(timeCol)).trim();
                        if (!timeStr.isEmpty()) {
                            try {
                                data.setCollectTime(LocalDateTime.parse(timeStr, formatter));
                            } catch (Exception e) {
                                throw new IllegalArgumentException("采集时间格式错误: '" + timeStr + "'，正确格式: yyyy-MM-dd HH:mm:ss");
                            }
                        } else {
                            data.setCollectTime(LocalDateTime.now());
                        }
                    } else {
                        data.setCollectTime(LocalDateTime.now());
                    }

                    data.setFillTime(LocalDateTime.now());

                    // 样本量(可选, 仅计数型图 P/NP/U 需要)
                    if (sampleSizeCol >= 0 && row.getCell(sampleSizeCol) != null) {
                        String szStr = getCellValue(row.getCell(sampleSizeCol)).trim();
                        if (!szStr.isEmpty()) {
                            try {
                                int sz = Integer.parseInt(szStr);
                                if (sz > 0) {
                                    data.setSampleSize(sz);
                                } else {
                                    throw new IllegalArgumentException("样本量必须为正整数: " + szStr);
                                }
                            } catch (NumberFormatException e) {
                                throw new IllegalArgumentException("样本量格式错误: '" + szStr + "'，请输入正整数");
                            }
                        }
                    }

                    // 走 uploadData 统一通道：自动解析当前版本、评估 OOC/OOS、缓存最新数据、触发判异
                    SpcData saved = spcDataService.uploadData(data, role);
                    if (saved != null) {
                        successList.add(saved);
                        log.debug("[Import] 第{}行导入成功: batch={} value={}", rowIndex + 1,
                                 saved.getBatchId(), saved.getMeasuredValue());
                    } else {
                        // uploadData 返回 null 表示重复消息被去重，按失败行处理
                        Map<String, String> failItem = new HashMap<>();
                        failItem.put("row", String.valueOf(rowIndex + 1));
                        failItem.put("reason", "重复数据被去重");
                        failList.add(failItem);
                    }
                } catch (IllegalArgumentException e) {
                    Map<String, String> failItem = new HashMap<>();
                    failItem.put("row", String.valueOf(rowIndex + 1));
                    failItem.put("reason", e.getMessage());
                    failList.add(failItem);
                    log.warn("[Import] 第{}行数据验证失败: {}", rowIndex + 1, e.getMessage());
                } catch (Exception e) {
                    Map<String, String> failItem = new HashMap<>();
                    failItem.put("row", String.valueOf(rowIndex + 1));
                    failItem.put("reason", "系统错误: " + e.getMessage());
                    failList.add(failItem);
                    log.error("[Import] 第{}行导入异常: {}", rowIndex + 1, e.getMessage(), e);
                }
            }

        } catch (IOException e) {
            log.error("[Import] 读取Excel文件失败: {}", e.getMessage(), e);
            result.put("error", "读取文件失败: " + e.getMessage());
            return result;
        }

        result.put("totalCount", successList.size() + failList.size());
        result.put("successCount", successList.size());
        result.put("failCount", failList.size());
        result.put("successList", successList);
        result.put("failList", failList);

        log.info("[Import] 导入完成: paramId={} productId={} total={} success={} fail={}",
                paramId, productId, successList.size() + failList.size(), successList.size(), failList.size());
        return result;
    }

    @Override
    public void exportToExcel(Long paramId, Long productId, Long equipmentId, Integer limit,
                              LocalDateTime startTime, LocalDateTime endTime,
                              HttpServletResponse response) throws IOException {
        ParamVersion version = paramVersionService.getCurrentVersion(paramId, productId);
        if (version == null) {
            throw new ResourceNotFoundException("参数当前版本", "paramId=" + paramId + ", productId=" + productId);
        }

        LambdaQueryWrapper<SpcData> wrapper = new LambdaQueryWrapper<SpcData>()
                .eq(SpcData::getParamVersionId, version.getId())
                .eq(equipmentId != null, SpcData::getEquipmentId, equipmentId)
                .ge(startTime != null, SpcData::getCollectTime, startTime)
                .le(endTime != null, SpcData::getCollectTime, endTime)
                .eq(SpcData::getDeleted, 0)
                .orderByDesc(SpcData::getCollectTime);
        if (limit != null && limit > 0) {
            wrapper.last("LIMIT " + limit);
        }

        List<SpcData> dataList = spcDataService.list(wrapper);

        response.setContentType("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
        String fileName = URLEncoder.encode(
                "SPC数据导出_" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss")) + ".xlsx",
                StandardCharsets.UTF_8.toString()
        );
        response.setHeader("Content-Disposition", "attachment; filename=" + fileName);

        try (Workbook workbook = new XSSFWorkbook()) {
            Sheet sheet = workbook.createSheet("SPC数据");

            CellStyle headerStyle = createHeaderStyle(workbook);
            CellStyle dateStyle = createDateStyle(workbook);

            Row headerRow = sheet.createRow(0);
            String[] headers = {"序号", "批次号", "测量值", "样本量", "采集时间", "录入时间", "OOC", "OOS"};
            for (int i = 0; i < headers.length; i++) {
                Cell cell = headerRow.createCell(i);
                cell.setCellValue(headers[i]);
                cell.setCellStyle(headerStyle);
            }

            for (int i = 0; i < dataList.size(); i++) {
                SpcData data = dataList.get(i);
                Row row = sheet.createRow(i + 1);
                row.createCell(0).setCellValue(i + 1);
                row.createCell(1).setCellValue(data.getBatchId() != null ? data.getBatchId() : "");
                if (data.getMeasuredValue() != null) {
                    row.createCell(2).setCellValue(data.getMeasuredValue().doubleValue());
                }
                if (data.getSampleSize() != null) {
                    row.createCell(3).setCellValue(data.getSampleSize());
                } else {
                    row.createCell(3).setCellValue("");
                }
                if (data.getCollectTime() != null) {
                    Cell timeCell = row.createCell(4);
                    timeCell.setCellValue(data.getCollectTime());
                    timeCell.setCellStyle(dateStyle);
                }
                if (data.getFillTime() != null) {
                    Cell fillCell = row.createCell(5);
                    fillCell.setCellValue(data.getFillTime());
                    fillCell.setCellStyle(dateStyle);
                }
                row.createCell(6).setCellValue(data.getIsOoc() != null ? data.getIsOoc() : 0);
                row.createCell(7).setCellValue(data.getIsOos() != null ? data.getIsOos() : 0);
            }

            for (int i = 0; i < headers.length; i++) {
                sheet.autoSizeColumn(i);
            }

            workbook.write(response.getOutputStream());
        }

        log.info("[Export] 导出完成: paramId={} productId={} paramVersionId={} count={}",
                paramId, productId, version.getId(), dataList.size());
    }

    @Override
    public void downloadTemplate(HttpServletResponse response) throws IOException {
        response.setContentType("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
        String fileName = URLEncoder.encode("SPC数据导入模板.xlsx", StandardCharsets.UTF_8.toString());
        response.setHeader("Content-Disposition", "attachment; filename=" + fileName);

        try (Workbook workbook = new XSSFWorkbook()) {
            Sheet sheet = workbook.createSheet("数据导入");

            CellStyle headerStyle = createHeaderStyle(workbook);

            Row headerRow = sheet.createRow(0);
            String[] headers = {"批次号", "测量值", "样本量", "采集时间"};
            for (int i = 0; i < headers.length; i++) {
                Cell cell = headerRow.createCell(i);
                cell.setCellValue(headers[i]);
                cell.setCellStyle(headerStyle);
            }

            Row exampleRow = sheet.createRow(1);
            exampleRow.createCell(0).setCellValue("BATCH-20260429-001");
            exampleRow.createCell(1).setCellValue(25.5);
            exampleRow.createCell(2).setCellValue("");
            exampleRow.createCell(3).setCellValue("2026-04-29 10:30:00");

            Row noteRow = sheet.createRow(3);
            noteRow.createCell(0).setCellValue("说明:");
            Row note1 = sheet.createRow(4);
            note1.createCell(0).setCellValue("- 批次号: 可选填");
            Row note2 = sheet.createRow(5);
            note2.createCell(0).setCellValue("- 测量值: 必填, 数值类型(计数图填不合格数/缺陷数)");
            Row note3 = sheet.createRow(6);
            note3.createCell(0).setCellValue("- 样本量: 计数型图(P/NP/U)必填, 连续型不填");
            Row note4 = sheet.createRow(7);
            note4.createCell(0).setCellValue("- 采集时间: 可选填, 格式 yyyy-MM-dd HH:mm:ss, 不填则使用当前时间");

            for (int i = 0; i < headers.length; i++) {
                sheet.autoSizeColumn(i);
            }

            workbook.write(response.getOutputStream());
        }
    }

    private String getCellValue(Cell cell) {
        if (cell == null) return "";
        switch (cell.getCellType()) {
            case STRING:
                return cell.getStringCellValue().trim();
            case NUMERIC:
                if (DateUtil.isCellDateFormatted(cell)) {
                    return cell.getLocalDateTimeCellValue().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                }
                double numVal = cell.getNumericCellValue();
                return numVal == Math.floor(numVal) ? String.valueOf((long) numVal) : String.valueOf(numVal);
            case BOOLEAN:
                return String.valueOf(cell.getBooleanCellValue());
            case FORMULA:
                try {
                    return cell.getStringCellValue();
                } catch (Exception e) {
                    return String.valueOf(cell.getNumericCellValue());
                }
            default:
                return "";
        }
    }

    private CellStyle createHeaderStyle(Workbook workbook) {
        CellStyle style = workbook.createCellStyle();
        Font font = workbook.createFont();
        font.setBold(true);
        style.setFont(font);
        style.setFillForegroundColor(IndexedColors.LIGHT_BLUE.getIndex());
        style.setFillPattern(FillPatternType.SOLID_FOREGROUND);
        style.setBorderBottom(BorderStyle.THIN);
        style.setBorderTop(BorderStyle.THIN);
        style.setBorderLeft(BorderStyle.THIN);
        style.setBorderRight(BorderStyle.THIN);
        return style;
    }

    private CellStyle createDateStyle(Workbook workbook) {
        CellStyle style = workbook.createCellStyle();
        CreationHelper creationHelper = workbook.getCreationHelper();
        style.setDataFormat(creationHelper.createDataFormat().getFormat("yyyy-MM-dd HH:mm:ss"));
        return style;
    }
}