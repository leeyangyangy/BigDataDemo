package xyz.leeyangy.spc;

import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;

public class PasswordGenerator {

    public static void main(String[] args) {
        BCryptPasswordEncoder encoder = new BCryptPasswordEncoder();

        String[] passwords = {"admin123", "123456", "operator123", "engineer123", "test123", "jxkj2023"};

        System.out.println("========== SPC 系统密码生成工具 ==========\n");

        for (String raw : passwords) {
            String encoded = encoder.encode(raw);
            System.out.println("原始密码: " + raw);
            System.out.println("BCrypt密文: " + encoded);
            System.out.println("验证结果: " + encoder.matches(raw, encoded));
            System.out.println("----------------------------------------");
        }

        System.out.println("\n========== 自定义密码生成 ==========");
        if (args.length > 0) {
            for (String raw : args) {
                String encoded = encoder.encode(raw);
                System.out.println(raw + " -> " + encoded);
            }
        } else {
            System.out.println("用法: java PasswordGenerator <password1> <password2> ...");
            System.out.println("或直接运行使用默认密码列表");
        }
    }
}
