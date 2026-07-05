package xyz.leeyangy.spc.service.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import xyz.leeyangy.spc.entity.YieldRate;
import xyz.leeyangy.spc.service.YieldService;
import xyz.leeyangy.spc.vo.YieldDataVO;

import javax.annotation.PostConstruct;
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

    /** 数据窗口天数：超过此天数的 summaries 被丢弃 */
    private static final int WINDOW_DAYS = 180;
    /** Redis Hash key：持久化最新 summaries 全集（field=summaryKey, value=summaryJson） */
    private static final String SUMMARIES_KEY = "spc:yield:summaries";
    /** Redis List key 前缀：持久化历史时序点（LPUSH + LTRIM 保留最近 100 条） */
    private static final String HISTORY_PREFIX = "spc:yield:history:";

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

    /**
     * 启动加载：从 Redis 恢复 summaries 和历史时序，避免重启后大屏空白等待消息
     * 加载失败不影响服务启动，会等待 RabbitMQ 推送
     */
    @PostConstruct
    public void loadFromRedis() {
        dataLock.lock();
        try {
            // 1. 恢复 summaries 并重建良率数据（含日期索引）
            Boolean exists = redisTemplate.hasKey(SUMMARIES_KEY);
            if (Boolean.TRUE.equals(exists)) {
                // 先做一次过期清理（处理停机期间过期的数据）
                LocalDateTime cutoff = LocalDateTime.now().minusDays(WINDOW_DAYS);
                cleanupExpiredKeys(Collections.emptySet(), cutoff);

                rebuildMemoryFromRedis();
                log.info("[Yield] 启动加载完成: 当前良率 {} 条, 历史序列 {} 个",
                        currentYieldRates.size(), historicalData.size());
            } else {
                log.info("[Yield] Redis 无 summaries 数据，等待 RabbitMQ 推送");
            }

            // 2. 加载历史时序（覆盖 rebuildMemoryFromRedis 中追加的最新点，避免重复）
            loadHistoryFromRedis();
        } catch (Exception e) {
            log.warn("[Yield] 启动加载失败（不影响服务启动）: {}", e.getMessage(), e);
        } finally {
            dataLock.unlock();
        }
    }

    /** 从 Redis List 加载历史时序，覆盖内存中追加的"启动点"，避免重复 */
    private void loadHistoryFromRedis() {
        ScanOptions options = ScanOptions.scanOptions()
                .match(HISTORY_PREFIX + "*").count(100).build();
        try (Cursor<String> cursor = redisTemplate.scan(options)) {
            int loaded = 0;
            while (cursor.hasNext()) {
                String hKey = cursor.next();
                String compositeKey = hKey.substring(HISTORY_PREFIX.length());
                List<String> points = redisTemplate.opsForList().range(hKey, 0, -1);
                if (points == null || points.isEmpty()) continue;
                // List 用 LPUSH，最新在前面，需反转回时间顺序
                Collections.reverse(points);
                List<YieldDataVO.YieldHistoryPoint> list = Collections.synchronizedList(new ArrayList<>());
                for (String p : points) {
                    try {
                        list.add(objectMapper.readValue(p, YieldDataVO.YieldHistoryPoint.class));
                    } catch (Exception ex) {
                        log.warn("[Yield] 反序列化历史点失败 key={}: {}", compositeKey, ex.getMessage());
                    }
                }
                historicalData.put(compositeKey, list);
                loaded++;
            }
            if (loaded > 0) {
                log.info("[Yield] 加载历史时序: {} 个序列", loaded);
            }
        } catch (Exception e) {
            log.warn("[Yield] 加载历史时序失败: {}", e.getMessage());
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void updateYieldData(Map<String, Map<String, Object>> summaries) {
        if (summaries == null || summaries.isEmpty()) {
            return;
        }
        log.info("[Yield] 收到 summaries 数量: {}", summaries.size());
        dataLock.lock();
        try {
            // ① 时间过滤：丢弃 last_test_time 超出窗口的条目
            LocalDateTime cutoff = LocalDateTime.now().minusDays(WINDOW_DAYS);
            Map<String, Map<String, Object>> valid = new LinkedHashMap<>();
            for (Map.Entry<String, Map<String, Object>> e : summaries.entrySet()) {
                if (isWithinWindow(e.getValue(), cutoff)) {
                    valid.put(e.getKey(), e.getValue());
                }
            }
            log.info("[Yield] 时间过滤: incoming={} valid={} (window={}d)",
                    summaries.size(), valid.size(), WINDOW_DAYS);
            if (valid.isEmpty()) {
                log.warn("[Yield] 时间过滤后无有效数据，跳过更新");
                return;
            }

            // ② Diff 写入 Redis Hash（覆盖同 key）
            persistSummariesToRedis(valid);

            // ③ 清理 Redis 中本次未覆盖且已过期的 key
            cleanupExpiredKeys(valid.keySet(), cutoff);

            // ④ 从 Redis 全量重建内存（保证内存与 Redis 一致）
            rebuildMemoryFromRedis();

            // ⑤ 数据更新后递增版本号，使旧缓存自然失效
            bumpCacheVersion();
            log.info("[Yield] 大屏数据更新成功: 当前良率 {} 条, 历史序列 {} 个",
                    currentYieldRates.size(), historicalData.size());
        } finally {
            dataLock.unlock();
        }
    }

    /** 判断 summary 的 last_test_time 是否在窗口内 */
    private boolean isWithinWindow(Map<String, Object> summary, LocalDateTime cutoff) {
        // 宽松策略：仅丢弃明确超出窗口的旧数据；缺失或无法解析时间戳的条目予以保留，
        // 与原 Python 服务（update_dashboard_data 不做任何时间过滤，全量接收）行为一致。
        if (summary == null) return true;
        Object lastTest = summary.get("last_test_time");
        if (lastTest == null) return true;
        try {
            LocalDateTime last = parseDateTime(String.valueOf(lastTest));
            return last.isAfter(cutoff);
        } catch (Exception e) {
            return true;
        }
    }

    /** 把本次窗口内的 summaries 写入 Redis Hash（覆盖同 key） */
    private void persistSummariesToRedis(Map<String, Map<String, Object>> valid) {
        try {
            Map<String, String> entries = new HashMap<>(valid.size());
            for (Map.Entry<String, Map<String, Object>> e : valid.entrySet()) {
                entries.put(e.getKey(), objectMapper.writeValueAsString(e.getValue()));
            }
            redisTemplate.opsForHash().putAll(SUMMARIES_KEY, entries);
            log.info("[Yield] summaries 写入 Redis Hash: {} 条", entries.size());
        } catch (Exception e) {
            log.error("[Yield] summaries 持久化失败: {}", e.getMessage(), e);
        }
    }

    /** 清理 Redis Hash 中本次未覆盖且已过期的 key（未过期则保留，容错生产方漏推） */
    private void cleanupExpiredKeys(Set<String> incomingKeys, LocalDateTime cutoff) {
        try {
            Set<Object> redisKeys = redisTemplate.opsForHash().keys(SUMMARIES_KEY);
            int removed = 0;
            for (Object k : redisKeys) {
                if (incomingKeys.contains(k)) continue;
                String json = (String) redisTemplate.opsForHash().get(SUMMARIES_KEY, k);
                if (json == null) continue;
                try {
                    Map<String, Object> s = objectMapper.readValue(json, Map.class);
                    if (!isWithinWindow(s, cutoff)) {
                        redisTemplate.opsForHash().delete(SUMMARIES_KEY, k);
                        removed++;
                    }
                } catch (Exception ignored) {
                    // 解析失败的脏数据直接清理
                    redisTemplate.opsForHash().delete(SUMMARIES_KEY, k);
                    removed++;
                }
            }
            if (removed > 0) {
                log.info("[Yield] 清理过期 summaries: {} 条", removed);
            }
        } catch (Exception e) {
            log.warn("[Yield] 清理过期 summaries 失败: {}", e.getMessage());
        }
    }

    /** 从 Redis 全量重建内存中的良率数据与历史时序 */
    @SuppressWarnings("unchecked")
    private void rebuildMemoryFromRedis() {
        try {
            Map<Object, Object> all = redisTemplate.opsForHash().entries(SUMMARIES_KEY);
            Map<String, Map<String, Object>> summaries = new LinkedHashMap<>(all.size());
            for (Map.Entry<Object, Object> e : all.entrySet()) {
                try {
                    summaries.put((String) e.getKey(),
                            objectMapper.readValue((String) e.getValue(), Map.class));
                } catch (Exception ex) {
                    log.warn("[Yield] 反序列化 summary 失败 key={}: {}", e.getKey(), ex.getMessage());
                }
            }

            CalcResult result = calculateYieldRates(summaries);

            currentYieldRates.clear();
            currentYieldRates.addAll(result.yieldRates);

            dateRanges.clear();
            dateRanges.putAll(result.dateRanges);

            productKeyIndex.clear();
            productKeyIndex.putAll(result.productKeyIndex);

            // 历史时序：追加本次计算结果，并持久化到 Redis List
            String timestamp = LocalDateTime.now()
                    .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
            for (YieldRate rate : result.yieldRates) {
                String compositeKey = rate.getProductName() + "-" + rate.getProductCode() + "-" + rate.getBinRank();
                YieldDataVO.YieldHistoryPoint point = YieldDataVO.YieldHistoryPoint.builder()
                        .timestamp(timestamp)
                        .yieldRate(rate.getYieldRate())
                        .build();
                // 内存追加 + 截断
                List<YieldDataVO.YieldHistoryPoint> list = historicalData.computeIfAbsent(
                        compositeKey, k -> Collections.synchronizedList(new ArrayList<>()));
                list.add(point);
                while (list.size() > HISTORY_MAX_SIZE) {
                    list.remove(0);
                }
                // Redis 持久化（LPUSH + LTRIM）
                try {
                    String hKey = HISTORY_PREFIX + compositeKey;
                    redisTemplate.opsForList().leftPush(hKey, objectMapper.writeValueAsString(point));
                    redisTemplate.opsForList().trim(hKey, 0, HISTORY_MAX_SIZE - 1);
                } catch (Exception ex) {
                    log.warn("[Yield] 历史时序持久化失败 key={}: {}", compositeKey, ex.getMessage());
                }
            }
        } catch (Exception e) {
            log.error("[Yield] 重建内存失败: {}", e.getMessage(), e);
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
        if (value == null || value.trim().isEmpty()) {
            throw new DateTimeParseException("empty timestamp", value, 0);
        }
        String v = value.trim();
        if (v.contains("T")) {
            // 带时区/偏移（如 ...Z 或 ...+08:00）按 ZonedDateTime 解析
            String s = v.replace("Z", "+00:00");
            try {
                return ZonedDateTime.parse(s).toLocalDateTime();
            } catch (DateTimeParseException e) {
                // 无偏移的本地时间（如 2026-06-27T10:30:00），与 Python fromisoformat 一致
                return LocalDateTime.parse(s);
            }
        }
        return LocalDateTime.parse(v, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
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
