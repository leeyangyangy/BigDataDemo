package xyz.leeyangy.spc.service;

import com.baomidou.mybatisplus.extension.service.IService;
import xyz.leeyangy.spc.entity.SysUser;

/**
 * 系统用户 Service 接口
 */
public interface SysUserService extends IService<SysUser> {

    SysUser getByEmpNo(String empNo);

    SysUser getByWecomUserId(String wecomUserId);

    boolean updatePassword(Long userId, String newPassword);

    boolean updateLoginInfo(Long userId, String ip);
}
