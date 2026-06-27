package xyz.leeyangy.spc.dto;

import lombok.Data;
import javax.validation.constraints.NotBlank;
import java.util.Map;

/**
 * 企业微信登录请求 DTO
 */
@Data
public class WechatLoginDTO {

    /** 企业微信授权码 */
    @NotBlank(message = "授权码不能为空")
    private String code;

    /** 用户信息（由前端透传，无强制校验） */
    private Map<String, Object> userInfo;
}
