package xyz.leeyangy.spc.common.constants;

/**
 * 系统角色常量。
 * 与数据库 sys_user.role 字段及 SecurityConfig 权限矩阵保持一致。
 */
public final class RoleConstants {

    private RoleConstants() {}

    public static final String ADMIN = "ADMIN";
    public static final String ENGINEER = "ENGINEER";
    public static final String OPERATOR = "OPERATOR";
    public static final String VIEWER = "VIEWER";

    /** SecurityConfig 中权限校验使用的角色前缀 */
    public static final String ROLE_PREFIX = "ROLE_";
}
