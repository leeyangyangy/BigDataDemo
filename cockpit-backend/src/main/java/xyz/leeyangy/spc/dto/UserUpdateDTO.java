package xyz.leeyangy.spc.dto;

import lombok.Data;

/**
 * 更新用户请求 DTO（字段全部可选）
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

    /** 车间ID */
    private Long workshopId;

    /** 是否清空车间绑定 */
    private boolean clearWorkshop;

    /** 状态 */
    private Integer status;
}
