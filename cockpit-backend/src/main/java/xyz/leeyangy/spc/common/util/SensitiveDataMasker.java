package xyz.leeyangy.spc.common.util;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 敏感数据脱敏工具
 *
 * <p>用于操作日志、异常信息、调试输出等场景，避免敏感信息明文落库或落日志。
 * 符合等保三级"数据保密性"要求 (GB/T 22239-2019)。</p>
 *
 * <p>支持脱敏类型：</p>
 * <ul>
 *   <li>手机号：138****8888</li>
 *   <li>邮箱：a***@example.com</li>
 *   <li>身份证号：110101********1234</li>
 *   <li>银行卡号：6222********1234</li>
 *   <li>密码：******</li>
 *   <li>姓名：张*</li>
 *   <li>地址：北京市朝阳区建国路***</li>
 * </ul>
 */
@Slf4j
@Component
public class SensitiveDataMasker {

    /** 敏感字段名集合（小写匹配） */
    private static final Set<String> SENSITIVE_FIELD_NAMES = new HashSet<>();

    /** 密码类字段名 */
    private static final Set<String> PASSWORD_FIELDS = new HashSet<>();

    /** 手机号正则 */
    private static final Pattern PHONE_PATTERN =
            Pattern.compile("(?<![0-9])(1[3-9]\\d{9})(?![0-9])");

    /** 邮箱正则 */
    private static final Pattern EMAIL_PATTERN =
            Pattern.compile("[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}");

    /** 身份证号正则（18位） */
    private static final Pattern ID_CARD_PATTERN =
            Pattern.compile("(?<![0-9])([1-9]\\d{5})(\\d{8})(\\d{3})([0-9Xx])(?![0-9])");

    /** 银行卡号正则（16-19位数字） */
    private static final Pattern BANK_CARD_PATTERN =
            Pattern.compile("(?<![0-9])(6\\d{15,18})(?![0-9])");

    static {
        // 密码类字段
        PASSWORD_FIELDS.add("password");
        PASSWORD_FIELDS.add("passwd");
        PASSWORD_FIELDS.add("pwd");
        PASSWORD_FIELDS.add("newpassword");
        PASSWORD_FIELDS.add("oldpassword");
        PASSWORD_FIELDS.add("confirmpassword");
        PASSWORD_FIELDS.add("originalpassword");
        PASSWORD_FIELDS.add("secret");
        PASSWORD_FIELDS.add("token");
        PASSWORD_FIELDS.add("accesstoken");
        PASSWORD_FIELDS.add("refreshtoken");
        PASSWORD_FIELDS.add("apikey");
        PASSWORD_FIELDS.add("apisecret");

        // 全部敏感字段
        SENSITIVE_FIELD_NAMES.addAll(PASSWORD_FIELDS);
        SENSITIVE_FIELD_NAMES.add("phone");
        SENSITIVE_FIELD_NAMES.add("mobile");
        SENSITIVE_FIELD_NAMES.add("telephone");
        SENSITIVE_FIELD_NAMES.add("email");
        SENSITIVE_FIELD_NAMES.add("idcard");
        SENSITIVE_FIELD_NAMES.add("idnumber");
        SENSITIVE_FIELD_NAMES.add("bankcard");
        SENSITIVE_FIELD_NAMES.add("cardnumber");
        SENSITIVE_FIELD_NAMES.add("cardno");
        SENSITIVE_FIELD_NAMES.add("name");
        SENSITIVE_FIELD_NAMES.add("username");
        SENSITIVE_FIELD_NAMES.add("realname");
        SENSITIVE_FIELD_NAMES.add("address");
        SENSITIVE_FIELD_NAMES.add("salt");
        SENSITIVE_FIELD_NAMES.add("key");
        SENSITIVE_FIELD_NAMES.add("aeskey");
        SENSITIVE_FIELD_NAMES.add("privatekey");
    }

    // ============ 单值脱敏方法 ============

    /** 手机号脱敏：138****8888 */
    public String maskPhone(String phone) {
        if (phone == null || phone.isEmpty()) {
            return phone;
        }
        String s = phone.trim();
        if (s.length() < 7) {
            return repeat('*', s.length());
        }
        return s.substring(0, 3) + "****" + s.substring(s.length() - 4);
    }

    /** 邮箱脱敏：a***@example.com */
    public String maskEmail(String email) {
        if (email == null || email.isEmpty()) {
            return email;
        }
        int at = email.indexOf('@');
        if (at <= 0) {
            return "***";
        }
        String local = email.substring(0, at);
        String domain = email.substring(at);
        if (local.length() <= 1) {
            return local + "***" + domain;
        }
        return local.charAt(0) + "***" + domain;
    }

    /** 身份证号脱敏：110101********1234 */
    public String maskIdCard(String idCard) {
        if (idCard == null || idCard.isEmpty()) {
            return idCard;
        }
        String s = idCard.trim();
        if (s.length() < 10) {
            return repeat('*', s.length());
        }
        return s.substring(0, 6) + repeat('*', s.length() - 10) + s.substring(s.length() - 4);
    }

    /** 银行卡号脱敏：6222********1234 */
    public String maskBankCard(String bankCard) {
        if (bankCard == null || bankCard.isEmpty()) {
            return bankCard;
        }
        String s = bankCard.trim().replace(" ", "");
        if (s.length() < 8) {
            return repeat('*', s.length());
        }
        return s.substring(0, 4) + repeat('*', s.length() - 8) + s.substring(s.length() - 4);
    }

    /** 密码脱敏：固定返回 ****** */
    public String maskPassword() {
        return "******";
    }

    /** 密码脱敏：固定返回 ******（兼容传参） */
    public String maskPassword(String ignored) {
        return "******";
    }

    /** 姓名脱敏：张* / 欧阳** */
    public String maskName(String name) {
        if (name == null || name.isEmpty()) {
            return name;
        }
        String s = name.trim();
        if (s.length() == 1) {
            return s;
        }
        if (s.length() == 2) {
            return s.charAt(0) + "*";
        }
        return s.charAt(0) + repeat('*', s.length() - 2) + s.charAt(s.length() - 1);
    }

    /** 地址脱敏：保留前6位，后面用*** */
    public String maskAddress(String address) {
        if (address == null || address.isEmpty()) {
            return address;
        }
        String s = address.trim();
        if (s.length() <= 6) {
            return s;
        }
        return s.substring(0, 6) + "***";
    }

    // ============ 按字段名智能脱敏 ============

    /**
     * 根据字段名智能选择脱敏策略。
     *
     * @param fieldName 字段名（不区分大小写）
     * @param value     字段值
     * @return 脱敏后的字符串
     */
    public String maskByFieldName(String fieldName, Object value) {
        if (value == null) {
            return null;
        }
        if (fieldName == null) {
            return value.toString();
        }
        String lowerName = fieldName.toLowerCase().replaceAll("[_-]", "");

        // 密码类：完全屏蔽
        if (PASSWORD_FIELDS.contains(lowerName)) {
            return "******";
        }

        String str = value.toString();
        if (str.isEmpty()) {
            return str;
        }

        // 按字段名匹配
        if (containsAny(lowerName, "phone", "mobile", "telephone", "tel")) {
            return maskPhone(str);
        }
        if (containsAny(lowerName, "email", "mail")) {
            return maskEmail(str);
        }
        if (containsAny(lowerName, "idcard", "idnumber", "identitycard", "certno")) {
            return maskIdCard(str);
        }
        if (containsAny(lowerName, "bankcard", "cardnumber", "cardno", "bankaccount")) {
            return maskBankCard(str);
        }
        if (containsAny(lowerName, "address", "addr")) {
            return maskAddress(str);
        }
        if ("name".equals(lowerName) || "realname".equals(lowerName)) {
            return maskName(str);
        }
        if (containsAny(lowerName, "salt", "key", "secret")) {
            return "******";
        }

        // 字段名不敏感，但内容可能是敏感数据（如操作日志中的请求体）
        return maskContent(str);
    }

    /**
     * 对字符串内容进行正则脱敏（识别其中出现的手机号/邮箱/身份证/银行卡）。
     */
    public String maskContent(String content) {
        if (content == null || content.isEmpty()) {
            return content;
        }
        String result = content;
        result = ID_CARD_PATTERN.matcher(result).replaceAll(match -> {
            String g = match.group();
            return g.substring(0, 6) + repeat('*', g.length() - 10) + g.substring(g.length() - 4);
        });
        result = BANK_CARD_PATTERN.matcher(result).replaceAll(match -> {
            String g = match.group();
            return g.substring(0, 4) + repeat('*', g.length() - 8) + g.substring(g.length() - 4);
        });
        result = PHONE_PATTERN.matcher(result).replaceAll(match -> {
            String g = match.group();
            return g.substring(0, 3) + "****" + g.substring(g.length() - 4);
        });
        result = EMAIL_PATTERN.matcher(result).replaceAll(match -> {
            String g = match.group();
            int at = g.indexOf('@');
            if (at <= 0) return "***";
            String local = g.substring(0, at);
            String domain = g.substring(at);
            return (local.length() <= 1 ? local : local.charAt(0) + "***") + domain;
        });
        return result;
    }

    // ============ JSON 字符串脱敏 ============

    /**
     * 脱敏 JSON 字符串中的敏感字段。
     *
     * <p>识别形如 {@code "password":"xxx"}、{@code "phone":"13812345678"} 的键值对，
     * 仅替换值部分，保留 JSON 结构。</p>
     */
    public String maskJson(String json) {
        if (json == null || json.isEmpty()) {
            return json;
        }
        // 匹配 "fieldName":"value"
        Pattern p = Pattern.compile(
                "\"([A-Za-z_][A-Za-z0-9_]*)\"\\s*:\\s*\"([^\"]*)\"");
        Matcher m = p.matcher(json);
        StringBuffer sb = new StringBuffer(json.length() + 16);
        while (m.find()) {
            String field = m.group(1);
            String value = m.group(2);
            String masked = maskByFieldName(field, value);
            // 仅在确实脱敏时替换（避免对非敏感字段做无意义改动）
            if (masked != null && !masked.equals(value)) {
                m.appendReplacement(sb, "\"" + field + "\":\"" + masked + "\"");
            }
        }
        m.appendTail(sb);
        // 再做一次内容正则脱敏（捕获非键值对形式的敏感数据）
        return maskContent(sb.toString());
    }

    // ============ 反射式对象脱敏 ============

    /**
     * 反射式脱敏对象中的敏感字段（递归处理）。
     *
     * <p>注意：此方法会修改原对象的字段值，仅在用于日志输出前的"快照"使用。
     * 若需保留原对象，请在调用前自行深拷贝。</p>
     *
     * @param obj 待脱敏对象
     * @param maxDepth 最大递归深度，避免循环引用
     */
    public void maskObject(Object obj, int maxDepth) {
        maskObjectInternal(obj, maxDepth, 0, new java.util.IdentityHashMap<>());
    }

    /** 默认递归深度 3 */
    public void maskObject(Object obj) {
        maskObject(obj, 3);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void maskObjectInternal(Object obj, int maxDepth, int depth, Map<Object, Object> visited) {
        if (obj == null || depth > maxDepth) {
            return;
        }
        // 跳过基本类型、String、包装类
        if (obj.getClass().isPrimitive() || obj instanceof String
                || obj instanceof Number || obj instanceof Boolean
                || obj instanceof Character) {
            return;
        }
        // 防止循环引用
        if (visited.containsKey(obj)) {
            return;
        }
        visited.put(obj, null);

        try {
            // Map 处理
            if (obj instanceof Map) {
                Map<String, Object> map = (Map<String, Object>) obj;
                for (Map.Entry<String, Object> entry : new HashSet<>(map.entrySet())) {
                    String key = entry.getKey();
                    Object val = entry.getValue();
                    if (val instanceof String) {
                        map.put(key, maskByFieldName(key, val));
                    } else {
                        maskObjectInternal(val, maxDepth, depth + 1, visited);
                    }
                }
                return;
            }

            // 反射字段处理
            Class<?> clazz = obj.getClass();
            while (clazz != null && clazz != Object.class) {
                for (Field field : clazz.getDeclaredFields()) {
                    if (java.lang.reflect.Modifier.isStatic(field.getModifiers())) {
                        continue;
                    }
                    if (field.getType() == String.class) {
                        try {
                            field.setAccessible(true);
                            String val = (String) field.get(obj);
                            if (val == null || val.isEmpty()) {
                                continue;
                            }
                            String masked = maskByFieldName(field.getName(), val);
                            if (masked != null && !masked.equals(val)) {
                                field.set(obj, masked);
                            }
                        } catch (Exception ignored) {
                            // 单字段脱敏失败不影响整体
                        }
                    } else if (!field.getType().isPrimitive()
                            && !field.getType().getName().startsWith("java.")) {
                        try {
                            field.setAccessible(true);
                            Object nested = field.get(obj);
                            maskObjectInternal(nested, maxDepth, depth + 1, visited);
                        } catch (Exception ignored) {
                            // 嵌套对象访问失败忽略
                        }
                    }
                }
                clazz = clazz.getSuperclass();
            }
        } catch (Exception e) {
            log.debug("[SensitiveDataMasker] maskObject 失败: {}", e.getMessage());
        }
    }

    // ============ 工具方法 ============

    private static String repeat(char ch, int count) {
        if (count <= 0) {
            return "";
        }
        char[] arr = new char[count];
        java.util.Arrays.fill(arr, ch);
        return new String(arr);
    }

    private static boolean containsAny(String src, String... targets) {
        for (String t : targets) {
            if (src.contains(t)) {
                return true;
            }
        }
        return false;
    }
}
