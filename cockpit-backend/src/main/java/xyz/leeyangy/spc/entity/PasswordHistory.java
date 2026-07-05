package xyz.leeyangy.spc.entity;

import com.baomidou.mybatisplus.annotation.TableName;
import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;
import lombok.EqualsAndHashCode;
import xyz.leeyangy.spc.common.BaseEntity;

import java.time.LocalDateTime;

/**
 * 密码历史记录
 *
 * <p>用于实现等保三级 "身份鉴别" 要求:
 * "当对用户进行身份鉴别时, 应采用口令、密码技术、生物技术等两种或两种以上组合的鉴别技术,
 *  并应确保口令具有一定的复杂度, 并定期更换; 应防止口令被篡改、猜测、暴力破解等攻击"</p>
 *
 * <p>通过记录用户最近 N 次密码哈希, 防止密码重用 (通常 N=5)。</p>
 */
@Data
@EqualsAndHashCode(callSuper = true)
@TableName("sys_password_history")
public class PasswordHistory extends BaseEntity {

    /** 用户 ID */
    private Long userId;

    /** 密码哈希 (BCrypt) */
    private String passwordHash;

    /** 创建时间 (即密码设置时间) */
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime createdAt;
}
