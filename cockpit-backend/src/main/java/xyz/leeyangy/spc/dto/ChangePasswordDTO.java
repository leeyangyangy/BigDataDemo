package xyz.leeyangy.spc.dto;

import lombok.Data;
import xyz.leeyangy.spc.common.validator.StrongPassword;

import javax.validation.constraints.NotBlank;

/**
 * 修改密码请求 DTO
 *
 * <p>新密码需符合强密码策略 (GB/T 22239-2019 等保三级要求):
 * 12-128位, 含大小写字母+数字+特殊字符, 禁止连续/重复字符。
 */
@Data
public class ChangePasswordDTO {

    /** 原密码 */
    @NotBlank(message = "原密码不能为空")
    private String oldPassword;

    /** 新密码 (需符合强密码策略) */
    @NotBlank(message = "新密码不能为空")
    @StrongPassword
    private String newPassword;
}
