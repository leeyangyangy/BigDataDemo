package xyz.leeyangy.spc.entity;

import com.baomidou.mybatisplus.annotation.TableName;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;
import lombok.EqualsAndHashCode;
import xyz.leeyangy.spc.common.BaseEntity;

import java.time.LocalDateTime;

@Data
@EqualsAndHashCode(callSuper = true)
@TableName("sys_user")
public class SysUser extends BaseEntity {

    private String empNo;
    private String username;
    @JsonIgnore
    private String password;
    /** 密码最后修改时间 (等保三级: 密码定期更换 90 天) */
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime passwordUpdatedAt;
    private String email;
    private String phone;
    private String avatar;
    private String role;
    private Long workshopId;
    private String wecomUserId;
    private Integer status;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime lastLoginAt;
    private String lastLoginIp;
}
