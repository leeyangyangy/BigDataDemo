package xyz.leeyangy.spc.service.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import xyz.leeyangy.spc.entity.YieldRate;
import xyz.leeyangy.spc.service.YieldService;
import xyz.leeyangy.spc.vo.YieldDataVO;

import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 良率监控服务实现：内存存储 + 老化前/老化后良率计算 + Redis 缓存
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class YieldServiceImpl implements YieldService {

    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;

    /** 良率数据所属车间（数据来源为测试站） */
    private static final String DEFAULT_WORKSHOP = "测试站";

    /** 通用测试阶段后缀 */
    private static final List<String> COMMON_STAGES = Arrays.asList(
            "封装后", "老化前", "老化后", "二次老化前", "二次老化后");

    /** 分等类型 */
    private static final Set<String> BIN_TYPES = new HashSet<>(Arrays.asList(
            "single", "bin1", "bin2", "bin3", "bin4", "bin5"));

    private static final int HISTORY_MAX_SIZE = 100;

    /** Redis 缓存版本号 key（数据更新时递增，使旧缓存自然失效） */
    private static final String CACHE_VERSION_KEY = "spc:yield:version";
    /** 缓存 key 前缀：spc:yield:{data|search}:{version}:{workshop}:{keyword}:{startDate}:{endDate} */
    private static final String CACHE_PREFIX = "spc:yield:";
    /** 缓存 TTL（秒）：版本失效保证新鲜度，TTL 仅用于清理过期版本残留 */
    private static final long CACHE_TTL_SECONDS = 60L;

    /** 当前良率列表 */
    private final List<YieldRate> currentYieldRates = Collections.synchronizedList(new ArrayList<>());

    /** 历史数据：key = "产品-片号-分等"，value = 时间序列 */
    private final Map<String, List<YieldDataVO.YieldHistoryPoint>> historicalData =
            new ConcurrentHashMap<>();

    /** 日期范围索引：key = RabbitMQ 摘要 key，value = 首末测试时间 */
    private final Map<String, Map<String, String>> dateRanges = new ConcurrentHashMap<>();

    /** 产品索引：key = (产品名, 片号)，value = 关联的摘要 key 列表 */
    private final Map<String, List<String>> productKeyIndex = new ConcurrentHashMap<>();

    private final ReentrantLock dataLock = new ReentrantLock();

    @Override
    @SuppressWarnings("unchecked")
    public void updateYieldData(Map<String, Map<String, Object>> summaries) {
        if (summaries == null || summaries.isEmpty()) {
            return;
        }
        log.info("[Yield] 开始计算良率，summaries 数量: {}", summaries.size());
        dataLock.lock();
        try {
            CalcResult result = calculateYieldRates(summaries);

            currentYieldRates.clear();
            currentYieldRates.addAll(result.yieldRates);

            dateRanges.clear();
            dateRanges.putAll(result.dateRanges);

            productKeyIndex.clear();
            productKeyIndex.putAll(result.productKeyIndex);

            String timestamp = LocalDateTime.now()
                    .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
            for (YieldRate rate : result.yieldRates) {
                String key = rate.getProductName() + "-" + rate.getProductCode() + "-" + rate.getBinRank();
                List<YieldDataVO.YieldHistoryPoint> list = historicalData.computeIfAbsent(
                        key, k -> Collections.synchronizedList(new ArrayList<>()));
                list.add(YieldDataVO.YieldHistoryPoint.builder()
                        .timestamp(timestamp)
                        .yieldRate(rate.getYieldRate())
                        .build());
                while (list.size() > HISTORY_MAX_SIZE) {
                    list.remove(0);
                }
            }
            log.info("[Yield] 大屏数据更新成功: 当前良率 {} 条, 历史序列 {} 个",
                    currentYieldRates.size(), historicalData.size());
            // 数据更新后递增版本号，使旧缓存自然失效（下次查询重新计算并回填缓存）
            bumpCacheVersion();
        } finally {
            dataLock.unlock();
        }
    }

    @Override
    public YieldDataVO getYieldData(String workshop, String startDate, String endDate) {
        String cacheKey = buildCacheKey("data", getCacheVersion(), workshop, null, startDate, endDate);
        YieldDataVO cached = readCache(cacheKey);
        if (cached != null) {
            return cached;
        }
        dataLock.lock();
        try {
            List<YieldRate> filtered = filterByWorkshop(currentYieldRates, workshop);
            if ((startDate != null && !startDate.isEmpty()) || (endDate != null && !endDate.isEmpty())) {
                Set<String> validKeys = collectValidKeys(startDate, endDate);
                filtered = filterByDateRange(filtered, validKeys);
            }
            YieldDataVO result = buildResponse(filtered);
            writeCache(cacheKey, result);
            return result;
        } finally {
            dataLock.unlock();
        }
    }

    @Override
    public YieldDataVO searchYieldData(String workshop, String keyword, String startDate, String endDate) {
        String cacheKey = buildCacheKey("search", getCacheVersion(), workshop, keyword, startDate, endDate);
        YieldDataVO cached = readCache(cacheKey);
        if (cached != null) {
            return cached;
        }
        dataLock.lock();
        try {
            List<YieldRate> base = filterByWorkshop(currentYieldRates, workshop);
            List<YieldRate> filtered;
            if (keyword == null || keyword.isEmpty()) {
                filtered = new ArrayList<>(base);
            } else {
                String kw = keyword.toLowerCase();
                filtered = new ArrayList<>();
                for (YieldRate rate : base) {
                    if ((rate.getProductName() != null && rate.getProductName().toLowerCase().contains(kw))
                            || (rate.getProductCode() != null && rate.getProductCode().toLowerCase().contains(kw))
                            || (rate.getBinRank() != null && rate.getBinRank().toLowerCase().contains(kw))) {
                        filtered.add(rate);
                    }
                }
            }
            if ((startDate != null && !startDate.isEmpty()) || (endDate != null && !endDate.isEmpty())) {
                Set<String> validKeys = collectValidKeys(startDate, endDate);
                filtered = filterByDateRange(filtered, validKeys);
            }
            YieldDataVO result = buildResponse(filtered);
            writeCache(cacheKey, result);
            return result;
        } finally {
            dataLock.unlock();
        }
    }

    @Override
    public List<String> getWorkshops() {
        List<String> workshops = new ArrayList<>();
        workshops.add(DEFAULT_WORKSHOP);
        return workshops;
    }

    // ==================== 核心计算逻辑（移植自 Python calculate_yield_rates） ====================

    @SuppressWarnings("unchecked")
    private CalcResult calculateYieldRates(Map<String, Map<String, Object>> summaries) {
        List<YieldRate> yieldRates = new ArrayList<>();
        Map<String, Map<String, String>> dateRanges = new HashMap<>();
        Map<String, List<String>> productKeyIndex = new HashMap<>();

        // 按 产品名-片号 分组，记录老化前/老化后摘要
        Map<String, Map<String, Map<String, Object>>> productCodeData = new HashMap<>();

        for (Map.Entry<String, Map<String, Object>> entry : summaries.entrySet()) {
            String key = entry.getKey();
            Map<String, Object> summary = entry.getValue();
            if (summary == null) {
                continue;
            }
            String[] parts = key.split("-");
            String stage = parts[parts.length - 1];

            // 记录日期范围
            Object firstTest = summary.get("first_test_time");
            Object lastTest = summary.get("last_test_time");
            if (firstTest != null && lastTest != null) {
                Map<String, String> range = new HashMap<>();
                range.put("first_test_time", String.valueOf(firstTest));
                range.put("last_test_time", String.valueOf(lastTest));
                dateRanges.put(key, range);
            }

            // 拆分出产品码部分（剥离阶段后缀）
            List<String> productCodePartsList = new ArrayList<>();
            for (int i = parts.length - 1; i >= 0; i--) {
                StringBuilder candidate = new StringBuilder();
                for (int j = i; j < parts.length; j++) {
                    if (j > i) candidate.append("-");
                    candidate.append(parts[j]);
                }
                if (COMMON_STAGES.contains(candidate.toString())) {
                    for (int k = 0; k < i; k++) {
                        productCodePartsList.add(parts[k]);
                    }
                    break;
                }
            }
            if (productCodePartsList.isEmpty()) {
                for (int i = 0; i < parts.length - 1; i++) {
                    productCodePartsList.add(parts[i]);
                }
            }

            // 拆分出分等类型、产品名、片号
            String productName = "未知产品";
            String binType = null;
            int binIndex = -1;
            for (int i = 0; i < productCodePartsList.size(); i++) {
                if (BIN_TYPES.contains(productCodePartsList.get(i).toLowerCase())) {
                    binType = productCodePartsList.get(i);
                    binIndex = i;
                    break;
                }
            }
            List<String> codeParts;
            if (binType != null) {
                if (binIndex > 0) {
                    productName = String.join("-", productCodePartsList.subList(0, binIndex));
                }
                codeParts = productCodePartsList.subList(binIndex + 1, productCodePartsList.size());
            } else {
                productName = "未知产品";
                codeParts = new ArrayList<>(productCodePartsList);
            }
            String productCode = String.join("-", codeParts);

            // 建立产品索引
            String indexKey = productName + "\u0000" + productCode;
            productKeyIndex.computeIfAbsent(indexKey, k -> new ArrayList<>()).add(key);

            // 按阶段归类
            String dataKey = productName + "-" + productCode;
            Map<String, Map<String, Object>> group = productCodeData
                    .computeIfAbsent(dataKey, k -> new HashMap<>());
            if ("老化前".equals(stage)) {
                group.put("before_aging", summary);
            } else if ("老化后".equals(stage)) {
                group.put("after_aging", summary);
            }
        }

        // 计算良率
        for (Map.Entry<String, Map<String, Map<String, Object>>> entry : productCodeData.entrySet()) {
            String dataKey = entry.getKey();
            Map<String, Map<String, Object>> data = entry.getValue();
            Map<String, Object> before = data.get("before_aging");
            Map<String, Object> after = data.get("after_aging");
            if (before == null || after == null) {
                continue;
            }
            Object beforeTotalObj = before.get("total");
            int beforeTotal = beforeTotalObj instanceof Number
                    ? ((Number) beforeTotalObj).intValue() : 0;
            if (beforeTotal == 0) {
                continue;
            }
            Object afterBinsObj = after.get("bins");
            if (!(afterBinsObj instanceof Map)) {
                continue;
            }
            Map<String, Object> afterBins = (Map<String, Object>) afterBinsObj;

            int sep = dataKey.indexOf('-');
            String productName = sep > 0 ? dataKey.substring(0, sep) : dataKey;
            String productCode = sep > 0 && sep < dataKey.length() - 1
                    ? dataKey.substring(sep + 1) : "";

            for (Map.Entry<String, Object> binEntry : afterBins.entrySet()) {
                String binRank = binEntry.getKey();
                int count = binEntry.getValue() instanceof Number
                        ? ((Number) binEntry.getValue()).intValue() : 0;
                double yieldRate = (count * 100.0) / beforeTotal;
                yieldRates.add(YieldRate.builder()
                        .workshop(DEFAULT_WORKSHOP)
                        .productName(productName)
                        .productCode(productCode)
                        .binRank(binRank)
                        .beforeTotal(beforeTotal)
                        .afterCount(count)
                        .yieldRate(Math.round(yieldRate * 100.0) / 100.0)
                        .build());
            }
        }
        return new CalcResult(yieldRates, dateRanges, productKeyIndex);
    }

    // ==================== 日期范围过滤 ====================

    private Set<String> collectValidKeys(String startDate, String endDate) {
        Set<String> validKeys = new HashSet<>();
        for (Map.Entry<String, Map<String, String>> entry : dateRanges.entrySet()) {
            Map<String, String> range = entry.getValue();
            if (isInDateRange(range.get("first_test_time"), range.get("last_test_time"), startDate, endDate)) {
                validKeys.add(entry.getKey());
            }
        }
        return validKeys;
    }

    @SuppressWarnings("unchecked")
    private List<YieldRate> filterByDateRange(List<YieldRate> rates, Set<String> validKeys) {
        if (validKeys.isEmpty()) {
            return new ArrayList<>(rates);
        }
        List<YieldRate> filtered = new ArrayList<>();
        for (YieldRate rate : rates) {
            String indexKey = rate.getProductName() + "\u0000" + rate.getProductCode();
            List<String> matchingKeys = productKeyIndex.get(indexKey);
            boolean hasValid = false;
            boolean hasAny = matchingKeys != null && !matchingKeys.isEmpty();
            if (hasAny) {
                for (String key : matchingKeys) {
                    if (validKeys.contains(key)) {
                        hasValid = true;
                        break;
                    }
                }
            }
            if (hasValid || !hasAny) {
                filtered.add(rate);
            }
        }
        return filtered;
    }

    private boolean isInDateRange(String firstTestTime, String lastTestTime, String startDate, String endDate) {
        if (firstTestTime == null || lastTestTime == null) {
            return false;
        }
        try {
            LocalDateTime firstDate = parseDateTime(firstTestTime);
            LocalDateTime lastDate = parseDateTime(lastTestTime);
            if (firstDate.isAfter(lastDate)) {
                LocalDateTime tmp = firstDate;
                firstDate = lastDate;
                lastDate = tmp;
            }
            if (startDate != null && !startDate.isEmpty()) {
                LocalDateTime start = parseLocalDate(startDate);
                if (lastDate.toLocalDate().isBefore(start.toLocalDate())) {
                    return false;
                }
            }
            if (endDate != null && !endDate.isEmpty()) {
                LocalDateTime end = parseLocalDate(endDate);
                if (firstDate.toLocalDate().isAfter(end.toLocalDate())) {
                    return false;
                }
            }
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private LocalDateTime parseDateTime(String value) {
        if (value.contains("T")) {
            return ZonedDateTime.parse(value.replace("Z", "+00:00"))
                    .toLocalDateTime();
        }
        return LocalDateTime.parse(value, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }

    private LocalDateTime parseLocalDate(String date) {
        return java.time.LocalDate.parse(date, DateTimeFormatter.ofPattern("yyyy-MM-dd")).atStartOfDay();
    }

    // ==================== 工具方法 ====================

    private List<YieldRate> filterByWorkshop(List<YieldRate> rates, String workshop) {
        if (workshop == null || workshop.isEmpty()) {
            return new ArrayList<>(rates);
        }
        List<YieldRate> filtered = new ArrayList<>();
        for (YieldRate rate : rates) {
            if (workshop.equals(rate.getWorkshop())) {
                filtered.add(rate);
            }
        }
        return filtered;
    }

    private YieldDataVO buildResponse(List<YieldRate> rates) {
        Map<String, List<YieldDataVO.YieldHistoryPoint>> history = new HashMap<>();
        for (YieldRate rate : rates) {
            String key = rate.getProductName() + "-" + rate.getProductCode() + "-" + rate.getBinRank();
            List<YieldDataVO.YieldHistoryPoint> points = historicalData.get(key);
            if (points != null) {
                history.put(key, new ArrayList<>(points));
            }
        }
        return YieldDataVO.builder()
                .currentYieldRates(rates)
                .historicalData(history)
                .build();
    }

    // ==================== Redis 缓存 ====================

    /** 获取当前缓存版本号（数据更新时递增） */
    private String getCacheVersion() {
        String v = redisTemplate.opsForValue().get(CACHE_VERSION_KEY);
        return v != null ? v : "0";
    }

    /** 数据更新后递增版本号，使旧版本缓存自然失效 */
    private void bumpCacheVersion() {
        try {
            redisTemplate.opsForValue().increment(CACHE_VERSION_KEY);
        } catch (Exception e) {
            log.warn("[Yield] 递增缓存版本号失败: {}", e.getMessage());
        }
    }

    /** 构建缓存 key：spc:yield:{type}:{version}:{workshop}:{keyword}:{startDate}:{endDate} */
    private String buildCacheKey(String type, String version, String workshop, String keyword,
                                 String startDate, String endDate) {
        return CACHE_PREFIX + type + ":" + version + ":"
                + (workshop == null ? "" : workshop) + ":"
                + (keyword == null ? "" : keyword) + ":"
                + (startDate == null ? "" : startDate) + ":"
                + (endDate == null ? "" : endDate);
    }

    /** 读取缓存（命中返回反序列化对象，未命中或异常返回 null） */
    private YieldDataVO readCache(String key) {
        try {
            String json = redisTemplate.opsForValue().get(key);
            if (json == null || json.isEmpty()) {
                return null;
            }
            return objectMapper.readValue(json, YieldDataVO.class);
        } catch (Exception e) {
            log.warn("[Yield] 缓存读取失败 key={}: {}", key, e.getMessage());
            return null;
        }
    }

    /** 写入缓存（序列化为 JSON，设置 TTL） */
    private void writeCache(String key, YieldDataVO data) {
        try {
            String json = objectMapper.writeValueAsString(data);
            redisTemplate.opsForValue().set(key, json, CACHE_TTL_SECONDS, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.warn("[Yield] 缓存写入失败 key={}: {}", key, e.getMessage());
        }
    }

    /** 计算中间结果 */
    private static class CalcResult {
        final List<YieldRate> yieldRates;
        final Map<String, Map<String, String>> dateRanges;
        final Map<String, List<String>> productKeyIndex;

        CalcResult(List<YieldRate> yieldRates,
                   Map<String, Map<String, String>> dateRanges,
                   Map<String, List<String>> productKeyIndex) {
            this.yieldRates = yieldRates;
            this.dateRanges = dateRanges;
            this.productKeyIndex = productKeyIndex;
        }
    }
}
