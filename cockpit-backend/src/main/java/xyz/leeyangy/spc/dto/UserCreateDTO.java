package xyz.leeyangy.spc.dto;

import lombok.Data;
import javax.validation.constraints.NotBlank;

/**
 * 创建用户请求 DTO
 *
 * 注: 车间绑定已迁移到数据中心权限页 (/api/admin/user-workshop),
 *     此 DTO 不再处理 workshopIds / primaryWorkshopId / testStationIds
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

    /** 状态 */
    private Integer status;
}

