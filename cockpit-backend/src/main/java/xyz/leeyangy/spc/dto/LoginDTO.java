package xyz.leeyangy.spc.dto;

import lombok.Data;
import javax.validation.constraints.NotBlank;

/**
 * 登录请求 DTO
 */
@Data
public class LoginDTO {

    /** 工号 */
    @NotBlank(message = "工号不能为空")
    private String empNo;

    /** 密码 */
    @NotBlank(message = "密码不能为空")
    private String password;
}
