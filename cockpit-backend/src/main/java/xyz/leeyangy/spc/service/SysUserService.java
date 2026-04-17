package xyz.leeyangy.spc.service;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import xyz.leeyangy.spc.entity.SysUser;
import xyz.leeyangy.spc.mapper.SysUserMapper;

@Slf4j
@Service
@RequiredArgsConstructor
public class SysUserService extends ServiceImpl<SysUserMapper, SysUser> {

    public SysUser getByEmpNo(String empNo) {
        return getOne(new LambdaQueryWrapper<SysUser>()
                .eq(SysUser::getEmpNo, empNo)
                .eq(SysUser::getStatus, 1)
                .eq(SysUser::getDeleted, 0));
    }

    public boolean updateLoginInfo(Long userId, String ip) {
        SysUser user = getById(userId);
        if (user == null) return false;
        user.setLastLoginAt(java.time.LocalDateTime.now());
        user.setLastLoginIp(ip);
        return updateById(user);
    }
}
