package xyz.leeyangy.spc.vo;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;
import xyz.leeyangy.spc.entity.SysUser;

import java.time.LocalDateTime;
import java.util.List;

@Data
public class SysUserVO {

    private Long id;
    private String empNo;
    private String username;
    private String email;
    private String phone;
    private String avatar;
    private String role;
    private Long workshopId;
    private String wecomUserId;
    private Integer status;

    /** 用户绑定的车间ID列表 (多车间管理) */
    private List<Long> workshopIds;

    /** 主车间ID (workshopIds 中标记为主车间) */
    private Long primaryWorkshopId;

    /** 用户绑定的测试站ID列表 */
    private List<Long> testStationIds;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime lastLoginAt;

    private String lastLoginIp;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime createdAt;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime updatedAt;

    private Long createdBy;
    private Long updatedBy;

    public static SysUserVO from(SysUser entity) {
        if (entity == null) {
            return null;
        }
        SysUserVO vo = new SysUserVO();
        vo.setId(entity.getId());
        vo.setEmpNo(entity.getEmpNo());
        vo.setUsername(entity.getUsername());
        vo.setEmail(entity.getEmail());
        vo.setPhone(entity.getPhone());
        vo.setAvatar(entity.getAvatar());
        vo.setRole(entity.getRole());
        vo.setWorkshopId(entity.getWorkshopId());
        vo.setWecomUserId(entity.getWecomUserId());
        vo.setStatus(entity.getStatus());
        vo.setLastLoginAt(entity.getLastLoginAt());
        vo.setLastLoginIp(entity.getLastLoginIp());
        vo.setCreatedAt(entity.getCreatedAt());
        vo.setUpdatedAt(entity.getUpdatedAt());
        vo.setCreatedBy(entity.getCreatedBy());
        vo.setUpdatedBy(entity.getUpdatedBy());
        return vo;
    }
}
