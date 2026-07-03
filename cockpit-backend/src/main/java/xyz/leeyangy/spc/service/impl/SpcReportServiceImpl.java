package xyz.leeyangy.spc.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.itextpdf.io.font.PdfEncodings;
import com.itextpdf.io.image.ImageDataFactory;
import com.itextpdf.kernel.colors.ColorConstants;
import com.itextpdf.kernel.colors.DeviceRgb;
import com.itextpdf.kernel.font.PdfFont;
import com.itextpdf.kernel.font.PdfFontFactory;
import com.itextpdf.kernel.geom.PageSize;
import com.itextpdf.kernel.pdf.PdfDocument;
import com.itextpdf.kernel.pdf.PdfWriter;
import com.itextpdf.kernel.pdf.canvas.draw.SolidLine;
import com.itextpdf.layout.Document;
import com.itextpdf.layout.borders.Border;
import com.itextpdf.layout.borders.SolidBorder;
import com.itextpdf.layout.element.Cell;
import com.itextpdf.layout.element.Image;
import com.itextpdf.layout.element.LineSeparator;
import com.itextpdf.layout.element.Paragraph;
import com.itextpdf.layout.element.Table;
import com.itextpdf.layout.properties.HorizontalAlignment;
import com.itextpdf.layout.properties.TextAlignment;
import com.itextpdf.layout.properties.UnitValue;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import xyz.leeyangy.spc.common.exception.BusinessException;
import xyz.leeyangy.spc.common.exception.ResourceNotFoundException;
import xyz.leeyangy.spc.entity.ParamVersion;
import xyz.leeyangy.spc.entity.SpcAlert;
import xyz.leeyangy.spc.entity.SpcData;
import xyz.leeyangy.spc.entity.SpcStatResult;
import xyz.leeyangy.spc.service.ParamVersionService;
import xyz.leeyangy.spc.service.SpcDataService;
import xyz.leeyangy.spc.service.SpcReportService;
import xyz.leeyangy.spc.service.SpcRuleEngine;
import xyz.leeyangy.spc.service.calculator.SpcCalculator;
import xyz.leeyangy.spc.vo.SpcReportData;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;

/**
 * SPC 分析报告 Service 实现
 *
 * <p>对标 SPC 统计过程控制分析报告 PDF:
 * 基本信息 / 统计信息 / 控制限 / 过程能力 / 异常分析 / 图表</p>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class SpcReportServiceImpl implements SpcReportService {

    private final SpcDataService spcDataService;
    private final ParamVersionService paramVersionService;
    private final SpcCalculator spcCalculator;
    private final SpcRuleEngine spcRuleEngine;

    private static final DateTimeFormatter DT_FMT = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm");
    private static final DateTimeFormatter FILE_FMT = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");

    /** 中文字体路径(优先 Windows 系统字体,跨平台回退到 font-asian CJK 字体) */
    private static final String CN_FONT_WIN = "C:/Windows/Fonts/simhei.ttf";
    private static final String CN_FONT_FALLBACK = "STSong-Light";
    private static final String CN_ENCODING_CJK = "UniGB-UCS2-H";

    @Override
    public void generatePdfReport(Long paramId, Long productId, Long equipmentId, Integer limit,
                                  LocalDateTime startTime, LocalDateTime endTime,
                                  SpcReportData.Charts charts,
                                  HttpServletResponse response) {
        // 1. 解析当前版本
        ParamVersion version = paramVersionService.getCurrentVersion(paramId, productId);
        if (version == null) {
            throw new ResourceNotFoundException("参数当前版本",
                    "paramId=" + paramId + ", productId=" + productId);
        }

        // 2. 拉取数据
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
        if (dataList.isEmpty()) {
            throw new BusinessException("所选范围内无 SPC 数据,无法生成报告");
        }
        // wrapper 按 collect_time DESC 返回（最新在前），反转成正序（旧→新）以便报告图表按时间方向渲染
        Collections.reverse(dataList);

        // 3. 计算统计量
        SpcStatResult stat = spcCalculator.computeStatistics(dataList, version,
                version.getId(), null, "REPORT");

        // 4. 判异检测
        List<SpcAlert> alerts;
        try {
            alerts = spcRuleEngine.detectRules(dataList, version);
            if (alerts == null) alerts = Collections.emptyList();
        } catch (Exception e) {
            log.warn("[Report] 判异检测异常,跳过: {}", e.getMessage());
            alerts = Collections.emptyList();
        }

        // 5. 组装报告数据
        SpcReportData reportData = SpcReportData.build(version, stat, dataList, alerts,
                charts != null ? charts : new SpcReportData.Charts());

        // 6. 在内存中生成 PDF(避免中途失败污染 response)
        byte[] pdfBytes;
        try (java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream()) {
            writePdf(reportData, baos);
            pdfBytes = baos.toByteArray();
        } catch (IOException e) {
            log.error("[Report] PDF 生成失败: {}", e.getMessage(), e);
            throw new BusinessException("PDF 生成失败: " + e.getMessage());
        }

        // 7. 全部成功后写入 response
        response.setContentType("application/pdf");
        String fileName = URLEncoder.encode(
                "SPC统计过程控制分析报告_" + LocalDateTime.now().format(FILE_FMT) + ".pdf",
                StandardCharsets.UTF_8);
        response.setHeader("Content-Disposition", "attachment; filename=" + fileName);
        response.setContentLength(pdfBytes.length);

        try (OutputStream os = response.getOutputStream()) {
            os.write(pdfBytes);
            os.flush();
        } catch (IOException e) {
            log.error("[Report] 写入响应流失败: {}", e.getMessage(), e);
            throw new BusinessException("PDF 输出失败: " + e.getMessage());
        }

        log.info("[Report] 报告生成成功: paramId={} productId={} paramVersionId={} sampleCount={} alerts={} size={}KB",
                paramId, productId, version.getId(), dataList.size(), alerts.size(), pdfBytes.length / 1024);
    }

    /**
     * 创建中文字体
     * <p>优先嵌入 Windows 系统字体 simhei.ttf(IDENTITY_H 编码,跨平台可显示);
     * 加载失败时回退到 font-asian 的 STSong-Light CJK 字体(UniGB-UCS2-H 编码)。</p>
     */
    private PdfFont createCnFont() {
        // 1. 优先尝试系统 TTF 字体(嵌入到 PDF,任何平台都能正确显示)
        java.io.File winFont = new java.io.File(CN_FONT_WIN);
        if (winFont.exists()) {
            try {
                return PdfFontFactory.createFont(CN_FONT_WIN, PdfEncodings.IDENTITY_H,
                        PdfFontFactory.EmbeddingStrategy.PREFER_EMBEDDED);
            } catch (IOException e) {
                log.warn("[Report] 系统字体 simhei.ttf 加载失败,回退 CJK 字体: {}", e.getMessage());
            }
        }
        // 2. 回退到 font-asian 的 STSong-Light
        try {
            return PdfFontFactory.createFont(CN_FONT_FALLBACK, CN_ENCODING_CJK);
        } catch (IOException e) {
            log.error("[Report] CJK 字体加载也失败,PDF 中文将无法显示: {}", e.getMessage());
            throw new RuntimeException("中文字体加载失败: " + e.getMessage(), e);
        }
    }

    /** 真正写 PDF 文档 */
    private void writePdf(SpcReportData data, OutputStream os) throws IOException {
        PdfWriter writer = new PdfWriter(os);
        PdfDocument pdf = new PdfDocument(writer);
        Document doc = new Document(pdf, PageSize.A4, false);
        doc.setMargins(36, 36, 36, 36);

        PdfFont cnFont = createCnFont();
        PdfFont cnBold = cnFont;
        doc.setFont(cnFont);

        // ============ 标题 ============
        Paragraph title = new Paragraph("SPC统计过程控制分析报告")
                .setFont(cnBold).setFontSize(20).setBold()
                .setTextAlignment(TextAlignment.CENTER)
                .setMarginBottom(4);
        doc.add(title);

        Paragraph genTime = new Paragraph("生成时间: " + data.getGenerateTime().format(DT_FMT))
                .setFontSize(9).setFontColor(ColorConstants.GRAY)
                .setTextAlignment(TextAlignment.CENTER)
                .setMarginBottom(12);
        doc.add(genTime);

        // ============ 基本信息 ============
        addSectionTitle(doc, cnBold, "基本信息");
        SpcReportData.BasicInfo basic = data.getBasicInfo();
        Table basicTable = new Table(UnitValue.createPercentArray(new float[]{3, 3, 2, 4}))
                .setWidth(UnitValue.createPercentValue(100))
                .setMarginBottom(12);
        addHeaderCell(basicTable, cnBold, "指标");
        addHeaderCell(basicTable, cnBold, "值");
        addHeaderCell(basicTable, cnBold, "状态");
        addHeaderCell(basicTable, cnBold, "说明");
        addBodyCell(basicTable, "控制图类型", basic.getChartTypeName(), "");
        addBodyCell(basicTable, "子组容量", String.valueOf(basic.getSubgroupSize()), "");
        addBodyCell(basicTable, "子组数量", String.valueOf(basic.getSubgroupCount()), "");
        addBodyCell(basicTable, "样本总数", String.valueOf(basic.getSampleCount()), "");
        addBodyCell(basicTable, "过程状态",
                basic.getProcessStatus(),
                "受控".equals(basic.getProcessStatus()) ? "✅ 优秀" : "⚠️ 失控");
        addBodyCell(basicTable, "测量系统评定", basic.getMeasurementAssessment(), "");
        doc.add(basicTable);

        // ============ 统计信息 ============
        addSectionTitle(doc, cnBold, "统计信息");
        SpcReportData.Statistics s = data.getStatistics();
        Table statTable = new Table(UnitValue.createPercentArray(new float[]{3, 3, 3, 3}))
                .setWidth(UnitValue.createPercentValue(100))
                .setMarginBottom(12);
        addHeaderCell(statTable, cnBold, "统计量");
        addHeaderCell(statTable, cnBold, "数值");
        addHeaderCell(statTable, cnBold, "统计量");
        addHeaderCell(statTable, cnBold, "数值");
        addBodyCellPair(statTable, "均值 (μ)", fmt(s.getMean()), "最小值", fmt(s.getMin()));
        addBodyCellPair(statTable, "标准差 σ_within", fmt(s.getStdDevWithin()), "最大值", fmt(s.getMax()));
        addBodyCellPair(statTable, "标准差 σ_overall", fmt(s.getStdDevOverall()), "中位数", fmt(s.getMedian()));
        addBodyCellPair(statTable, "极差", fmt(s.getRange()), "正态性 W", fmt(s.getNormalityW()));
        addBodyCellPair(statTable, "正态性 p-value", fmt(s.getNormalityPValue()),
                "是否正态", s.getIsNormal() == null ? "-" : (s.getIsNormal() ? "是" : "否"));
        doc.add(statTable);

        // ============ 控制限 ============
        addSectionTitle(doc, cnBold, "控制限");
        SpcReportData.ControlLimits cl = data.getControlLimits();
        Table clTable = new Table(UnitValue.createPercentArray(new float[]{3, 3, 3, 3}))
                .setWidth(UnitValue.createPercentValue(100))
                .setMarginBottom(12);
        addHeaderCell(clTable, cnBold, "项目");
        addHeaderCell(clTable, cnBold, "数值");
        addHeaderCell(clTable, cnBold, "项目");
        addHeaderCell(clTable, cnBold, "数值");
        addBodyCellPair(clTable, "上控制限 (UCL)", fmt(cl.getUcl()), "上规格限 (USL)", fmt(cl.getUsl()));
        addBodyCellPair(clTable, "中心线 (CL)", fmt(cl.getCl()), "下规格限 (LSL)", fmt(cl.getLsl()));
        addBodyCellPair(clTable, "下控制限 (LCL)", fmt(cl.getLcl()), "目标值 (Target)", fmt(cl.getTarget()));
        doc.add(clTable);

        // ============ 过程能力分析 ============
        addSectionTitle(doc, cnBold, "过程能力分析");
        SpcReportData.ProcessCapability cap = data.getCapability();
        Table capTable = new Table(UnitValue.createPercentArray(new float[]{3, 3, 2, 4}))
                .setWidth(UnitValue.createPercentValue(100))
                .setMarginBottom(6);
        addHeaderCell(capTable, cnBold, "指标");
        addHeaderCell(capTable, cnBold, "数值");
        addHeaderCell(capTable, cnBold, "评价");
        addHeaderCell(capTable, cnBold, "说明");
        addCapabilityRow(capTable, "Cp", cap.getCp());
        addCapabilityRow(capTable, "Cpk", cap.getCpk());
        addCapabilityRow(capTable, "Cpm", cap.getCpm());
        addCapabilityRow(capTable, "Pp", cap.getPp());
        addCapabilityRow(capTable, "Ppk", cap.getPpk());
        doc.add(capTable);

        Paragraph evalStandard = new Paragraph(
                "评价标准: ≥1.67 优秀, 1.33-1.67 良好, 1.00-1.33 合格, <1.00 不足")
                .setFontSize(8).setFontColor(ColorConstants.GRAY)
                .setMarginBottom(6);
        doc.add(evalStandard);

        Paragraph passInfo = new Paragraph(String.format(
                "合格数: %d / 不合格数: %d / 合格率: %s%%",
                cap.getPassCount() == null ? 0 : cap.getPassCount(),
                cap.getFailCount() == null ? 0 : cap.getFailCount(),
                fmt(cap.getPassRate())))
                .setFontSize(9)
                .setMarginBottom(12);
        doc.add(passInfo);

        // ============ 异常分析 ============
        addSectionTitle(doc, cnBold, "异常分析");
        SpcReportData.AnomalyAnalysis anomaly = data.getAnomaly();
        String anomalyColor = anomaly.isHasAnomaly() ? "#D32F2F" : "#2E7D32";
        Paragraph anomalySummary = new Paragraph(anomaly.getSummary())
                .setFontColor(parseColor(anomalyColor))
                .setMarginBottom(12);
        doc.add(anomalySummary);
        if (anomaly.isHasAnomaly() && anomaly.getAlerts() != null) {
            Table alertTable = new Table(UnitValue.createPercentArray(new float[]{2, 3, 3, 4}))
                    .setWidth(UnitValue.createPercentValue(100))
                    .setMarginBottom(12);
            addHeaderCell(alertTable, cnBold, "规则编号");
            addHeaderCell(alertTable, cnBold, "规则名称");
            addHeaderCell(alertTable, cnBold, "触发值");
            addHeaderCell(alertTable, cnBold, "告警时间");
            for (SpcAlert alert : anomaly.getAlerts()) {
                addCell(alertTable, alert.getRuleNumber() == null ? "" : alert.getRuleNumber());
                addCell(alertTable, alert.getRuleName() == null ? "" : alert.getRuleName());
                addCell(alertTable, alert.getMeasuredValue() == null
                        ? "-" : alert.getMeasuredValue().setScale(4, BigDecimal.ROUND_HALF_UP).toPlainString());
                addCell(alertTable, alert.getAlertTime() == null
                        ? "" : alert.getAlertTime().format(DT_FMT));
            }
            doc.add(alertTable);
        }

        // ============ 分析图表 ============
        addSectionTitle(doc, cnBold, "分析图表");
        SpcReportData.Charts charts = data.getCharts();
        addChart(doc, charts.getControlChartImr(), "SPC控制图 - 单值移动极差控制图");
        addChart(doc, charts.getControlChartXbar(), "X-bar控制图 - 过程均值监控");
        addChart(doc, charts.getControlChartR(), "R控制图 - 过程变异监控");
        addChart(doc, charts.getCapabilityHistogram(), "过程能力分析图 - 直方图");
        addChart(doc, charts.getNormalProbabilityPlot(), "正态概率图 - 正态性检验");
        addChart(doc, charts.getTrendChart(), "趋势图 - 数据趋势分析");

        // ============ 页脚 ============
        doc.add(new LineSeparator(new SolidLine(0.5f)).setMarginTop(12));
        Paragraph footer = new Paragraph("© 2025 飓芯科技 · SPC 过程控制分析系统")
                .setFontSize(8).setFontColor(ColorConstants.GRAY)
                .setTextAlignment(TextAlignment.CENTER)
                .setMarginTop(6);
        doc.add(footer);

        doc.close();
    }

    private void addSectionTitle(Document doc, PdfFont font, String text) {
        Paragraph p = new Paragraph(text)
                .setFont(font).setFontSize(13).setBold()
                .setFontColor(ColorConstants.WHITE)
                .setBackgroundColor(new DeviceRgb(33, 150, 243))
                .setPadding(5)
                .setMarginTop(8)
                .setMarginBottom(8);
        doc.add(p);
    }

    private void addHeaderCell(Table table, PdfFont font, String text) {
        Cell cell = new Cell()
                .setFont(font).setBold()
                .setBackgroundColor(new DeviceRgb(237, 242, 247))
                .setTextAlignment(TextAlignment.CENTER)
                .setPadding(5)
                .add(new Paragraph(text).setFontSize(9));
        table.addHeaderCell(cell);
    }

    private void addCell(Table table, String text) {
        Cell cell = new Cell()
                .setTextAlignment(TextAlignment.CENTER)
                .setPadding(4)
                .add(new Paragraph(text == null ? "-" : text).setFontSize(9));
        table.addCell(cell);
    }

    private void addBodyCell(Table table, String label, String value, String status) {
        addCell(table, label);
        addCell(table, value);
        addCell(table, status);
        addCell(table, "");
    }

    private void addBodyCellPair(Table table, String l1, String v1, String l2, String v2) {
        addCell(table, l1);
        addCell(table, v1);
        addCell(table, l2);
        addCell(table, v2);
    }

    private void addCapabilityRow(Table table, String label, BigDecimal value) {
        addCell(table, label);
        addCell(table, fmt(value));
        String eval = evaluate(value);
        Cell evalCell = new Cell()
                .setTextAlignment(TextAlignment.CENTER)
                .setPadding(4)
                .setFontColor(parseColor(evalColor(eval)))
                .add(new Paragraph(eval).setFontSize(9));
        table.addCell(evalCell);
        addCell(table, "");
    }

    private void addChart(Document doc, String base64, String caption) {
        if (base64 == null || base64.isEmpty()) {
            log.warn("[Report] 图表为空: {} (base64=null={})", caption, base64 == null);
            Paragraph p = new Paragraph("[" + caption + " - 未提供]")
                    .setFontSize(9).setFontColor(ColorConstants.GRAY)
                    .setTextAlignment(TextAlignment.CENTER)
                    .setMarginTop(8).setMarginBottom(8);
            doc.add(p);
            return;
        }
        try {
            String pure = base64;
            int idx = base64.indexOf("base64,");
            if (idx >= 0) pure = base64.substring(idx + 7);
            byte[] bytes = java.util.Base64.getDecoder().decode(pure);
            Image img = new Image(ImageDataFactory.create(bytes))
                    .setHorizontalAlignment(HorizontalAlignment.CENTER)
                    .setMaxWidth(UnitValue.createPercentValue(85))
                    .setMarginTop(8);
            // 自动缩放到页面宽度
            img.setAutoScale(true);
            doc.add(img);
            Paragraph cap = new Paragraph(caption)
                    .setFontSize(9).setFontColor(ColorConstants.GRAY)
                    .setTextAlignment(TextAlignment.CENTER)
                    .setMarginBottom(8);
            doc.add(cap);
        } catch (Exception e) {
            log.warn("[Report] 图表嵌入失败: {} - {}", caption, e.getMessage());
            Paragraph p = new Paragraph("[" + caption + " - 嵌入失败]")
                    .setFontSize(9).setFontColor(ColorConstants.RED)
                    .setTextAlignment(TextAlignment.CENTER);
            doc.add(p);
        }
    }

    private static String fmt(BigDecimal v) {
        return v == null ? "-" : v.setScale(4, BigDecimal.ROUND_HALF_UP).toPlainString();
    }

    private static String evaluate(BigDecimal v) {
        if (v == null) return "无法评价";
        double d = v.doubleValue();
        if (d >= 1.67) return "优秀";
        if (d >= 1.33) return "良好";
        if (d >= 1.00) return "合格";
        return "不足";
    }

    private static String evalColor(String eval) {
        switch (eval) {
            case "优秀": return "#2E7D32";
            case "良好": return "#1976D2";
            case "合格": return "#F57C00";
            case "不足": return "#D32F2F";
            default: return "#757575";
        }
    }

    private static com.itextpdf.kernel.colors.Color parseColor(String hex) {
        if (hex == null || hex.isEmpty()) return ColorConstants.GRAY;
        String h = hex.startsWith("#") ? hex.substring(1) : hex;
        try {
            int r = Integer.parseInt(h.substring(0, 2), 16);
            int g = Integer.parseInt(h.substring(2, 4), 16);
            int b = Integer.parseInt(h.substring(4, 6), 16);
            return new DeviceRgb(r, g, b);
        } catch (Exception e) {
            return ColorConstants.GRAY;
        }
    }
}
