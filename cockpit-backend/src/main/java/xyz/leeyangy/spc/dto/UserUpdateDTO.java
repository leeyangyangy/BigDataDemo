package xyz.leeyangy.spc.dto;

import lombok.Data;

/**
 * 更新用户请求 DTO（字段全部可选）
 *
 * 注: 车间绑定已迁移到数据中心权限页 (/api/admin/user-workshop),
 *     此 DTO 不再处理 workshopId / workshopIds / primaryWorkshopId / testStationIds
 */
@Data
public class UserUpdateDTO {

    /** 姓名 */
    private String username;

    /** 密码 */
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

