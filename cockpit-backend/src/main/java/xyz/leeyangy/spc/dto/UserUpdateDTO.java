package xyz.leeyangy.spc.dto;

import lombok.Data;
import xyz.leeyangy.spc.common.validator.StrongPassword;

import javax.validation.constraints.Email;
import javax.validation.constraints.Pattern;
import javax.validation.constraints.Size;

/**
 * 更新用户请求 DTO（字段全部可选）
 *
 * <p>注: 车间绑定已迁移到数据中心权限页 (/api/admin/user-workshop),
 *     此 DTO 不再处理 workshopId / workshopIds / primaryWorkshopId / testStationIds
 */
@Data
public class UserUpdateDTO {

    /** 姓名 */
    @Size(min = 2, max = 30, message = "姓名长度2-30位")
    private String username;

    /** 密码 (可选, 提供时需符合强密码策略) */
    @StrongPassword
    private String password;

    /** 邮箱 */
    @Email(message = "邮箱格式不正确")
    @Size(max = 100, message = "邮箱长度不能超过100")
    private String email;

    /** 电话 */
    @Pattern(regexp = "^(|$|1[3-9]\\d{9}$|0\\d{2,3}-?\\d{7,8}$)",
            message = "电话格式不正确")
    private String phone;

    /** 角色 */
    @Pattern(regexp = "^(ADMIN|ENGINEER|OPERATOR|VIEWER)$",
            message = "角色必须为 ADMIN/ENGINEER/OPERATOR/VIEWER 之一")
    private String role;

    /** 状态 */
    @Pattern(regexp = "^[01]$", message = "状态必须为 0 或 1")
    private Integer status;
}
