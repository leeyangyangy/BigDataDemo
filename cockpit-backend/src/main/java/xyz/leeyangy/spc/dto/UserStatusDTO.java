package xyz.leeyangy.spc.dto;

import lombok.Data;

/**
 * 用户状态变更请求 DTO（status 可选，未传时由 Controller 翻转状态）
 */
@Data
public class UserStatusDTO {

    /** 状态 */
    private Integer status;
}
