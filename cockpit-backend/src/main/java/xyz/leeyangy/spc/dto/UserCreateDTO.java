package xyz.leeyangy.spc.dto;

import lombok.Data;
import javax.validation.constraints.NotBlank;

/**
 * 创建用户请求 DTO
 */
@Data
public class UserCreateDTO {

    /** 工号 */
    @NotBlank(message = "工号不能为空")
    private String empNo;

    /** 姓名 */
    @NotBlank(message = "姓名不能为空")
    private String username;

    /** 密码 */
    @NotBlank(message = "密码不能为空")
    private String password;

    /** 邮箱 */
    private String email;

    /** 电话 */
    private String phone;

    /** 角色 */
    private String role;

    /** 车间ID */
    private Long workshopId;

    /** 状态 */
    private Integer status;
}
