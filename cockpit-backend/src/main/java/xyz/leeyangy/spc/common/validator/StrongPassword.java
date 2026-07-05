package xyz.leeyangy.spc.common.validator;

import javax.validation.Constraint;
import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import javax.validation.Payload;
import java.lang.annotation.*;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * 强密码校验注解 (符合 GB/T 22239-2019 等保三级 身份鉴别复杂度要求)。
 *
 * <p>校验规则:
 * <ul>
 *   <li>长度 12-128 字符</li>
 *   <li>必须包含大写字母、小写字母、数字、特殊字符 各至少1个</li>
 *   <li>禁止连续递增/递减字符 (如 1234, abcd, dcba)</li>
 *   <li>禁止连续重复字符 (如 aaaa, 1111)</li>
 *   <li>禁止常见弱密码 (黑名单)</li>
 * </ul>
 *
 * <p>使用方式:
 * <pre>
 * &#64;StrongPassword
 * private String password;
 * </pre>
 *
 * @author SPC System
 * @since 2026-04-21
 */
@Target({ElementType.FIELD, ElementType.PARAMETER, ElementType.ANNOTATION_TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Constraint(validatedBy = StrongPassword.Validator.class)
@Documented
public @interface StrongPassword {

    /** 默认错误消息 */
    String message() default "密码必须12-128位, 且包含大小写字母、数字和特殊字符, 禁止连续/重复字符";

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};

    /**
     * 强密码校验器实现。
     */
    class Validator implements ConstraintValidator<StrongPassword, String> {

        // 正则规则: 长度+四类字符
        private static final Pattern LENGTH_PATTERN =
                Pattern.compile("^.{12,128}$");
        private static final Pattern LOWER_CASE_PATTERN =
                Pattern.compile(".*[a-z].*");
        private static final Pattern UPPER_CASE_PATTERN =
                Pattern.compile(".*[A-Z].*");
        private static final Pattern DIGIT_PATTERN =
                Pattern.compile(".*\\d.*");
        private static final Pattern SPECIAL_CHAR_PATTERN =
                Pattern.compile(".*[!@#$%^&*()_+\\-=\\[\\]{};':\"\\\\|,.<>/?~`].*");

        // 弱密码黑名单 (示例, 可按需扩充)
        private static final Set<String> WEAK_PASSWORD_BLACKLIST = new HashSet<>();
        static {
            WEAK_PASSWORD_BLACKLIST.add("Password123!");
            WEAK_PASSWORD_BLACKLIST.add("Admin@12345");
            WEAK_PASSWORD_BLACKLIST.add("Qwerty@123");
            WEAK_PASSWORD_BLACKLIST.add("Welcome@123");
            WEAK_PASSWORD_BLACKLIST.add("P@ssw0rd123!");
            WEAK_PASSWORD_BLACKLIST.add("Admin@2024");
            WEAK_PASSWORD_BLACKLIST.add("Admin@2025");
            WEAK_PASSWORD_BLACKLIST.add("Admin@2026");
            WEAK_PASSWORD_BLACKLIST.add("Test@12345");
            WEAK_PASSWORD_BLACKLIST.add("Abcd@1234");
            WEAK_PASSWORD_BLACKLIST.add("Root@12345");
            WEAK_PASSWORD_BLACKLIST.add("User@12345");
        }

        @Override
        public boolean isValid(String password, ConstraintValidatorContext ctx) {
            if (password == null || password.trim().isEmpty()) {
                return false;
            }

            // 1. 长度校验
            if (!LENGTH_PATTERN.matcher(password).matches()) {
                return false;
            }

            // 2. 字符种类校验
            if (!LOWER_CASE_PATTERN.matcher(password).matches()
                    || !UPPER_CASE_PATTERN.matcher(password).matches()
                    || !DIGIT_PATTERN.matcher(password).matches()
                    || !SPECIAL_CHAR_PATTERN.matcher(password).matches()) {
                return false;
            }

            // 3. 黑名单校验
            if (WEAK_PASSWORD_BLACKLIST.contains(password)) {
                return false;
            }

            // 4. 连续字符校验 (如 1234, abcd)
            if (hasConsecutiveChars(password, 4)) {
                return false;
            }

            // 5. 重复字符校验 (如 aaaa, 1111)
            if (hasRepeatingChars(password, 4)) {
                return false;
            }

            // 6. 键盘连续序列校验 (如 qwerty, asdfgh)
            if (hasKeyboardSequence(password, 4)) {
                return false;
            }

            return true;
        }

        /**
         * 检查是否存在连续递增或递减的字符序列。
         */
        private boolean hasConsecutiveChars(String s, int len) {
            if (s.length() < len) return false;
            for (int i = 0; i <= s.length() - len; i++) {
                boolean asc = true, desc = true;
                for (int j = 0; j < len - 1; j++) {
                    int diff = s.charAt(i + j + 1) - s.charAt(i + j);
                    if (diff != 1) asc = false;
                    if (diff != -1) desc = false;
                    if (!asc && !desc) break;
                }
                if (asc || desc) return true;
            }
            return false;
        }

        /**
         * 检查是否存在连续重复的字符。
         */
        private boolean hasRepeatingChars(String s, int len) {
            if (s.length() < len) return false;
            for (int i = 0; i <= s.length() - len; i++) {
                boolean repeat = true;
                char first = s.charAt(i);
                for (int j = 1; j < len; j++) {
                    if (s.charAt(i + j) != first) {
                        repeat = false;
                        break;
                    }
                }
                if (repeat) return true;
            }
            return false;
        }

        /**
         * 检查键盘连续序列 (横向/斜向)。
         */
        private boolean hasKeyboardSequence(String s, int len) {
            String[] keyboardRows = {
                    "qwertyuiop", "asdfghjkl", "zxcvbnm",
                    "1234567890", "!@#$%^&*()"
            };
            String lower = s.toLowerCase();
            for (String row : keyboardRows) {
                for (int i = 0; i <= row.length() - len; i++) {
                    String forward = row.substring(i, i + len);
                    String reverse = new StringBuilder(forward).reverse().toString();
                    if (lower.contains(forward) || lower.contains(reverse)) {
                        return true;
                    }
                }
            }
            return false;
        }
    }
}
