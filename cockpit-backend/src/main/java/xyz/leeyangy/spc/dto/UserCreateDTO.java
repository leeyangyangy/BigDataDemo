package xyz.leeyangy.spc.dto;

import lombok.Data;
import xyz.leeyangy.spc.common.validator.StrongPassword;

import javax.validation.constraints.Email;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Pattern;
import javax.validation.constraints.Size;

/**
 * 创建用户请求 DTO
 *
 * <p>注: 车间绑定已迁移到数据中心权限页 (/api/admin/user-workshop),
 *     此 DTO 不再处理 workshopIds / primaryWorkshopId / testStationIds
 */
@Data
public class UserCreateDTO {

    /** 工号 (3-20位字母数字下划线) */
    @NotBlank(message = "工号不能为空")
    @Size(min = 3, max = 20, message = "工号长度3-20位")
    @Pattern(regexp = "^[a-zA-Z0-9_]+$", message = "工号只能包含字母、数字和下划线")
    private String empNo;

    /** 姓名 (2-30位) */
    @NotBlank(message = "姓名不能为空")
    @Size(min = 2, max = 30, message = "姓名长度2-30位")
    private String username;

    /** 密码 (需符合强密码策略) */
    @NotBlank(message = "密码不能为空")
    @StrongPassword
    private String password;

    /** 邮箱 */
    @Email(message = "邮箱格式不正确")
    @Size(max = 100, message = "邮箱长度不能超过100")
    private String email;

    /** 电话 (可选格式: 手机号或带分隔符的固话) */
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

