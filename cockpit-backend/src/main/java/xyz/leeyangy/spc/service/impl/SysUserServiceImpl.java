package xyz.leeyangy.spc.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;
import xyz.leeyangy.spc.entity.SysUser;
import xyz.leeyangy.spc.mapper.SysUserMapper;
import xyz.leeyangy.spc.service.SysUserService;

import java.time.LocalDateTime;

@Slf4j
@Service
@RequiredArgsConstructor
public class SysUserServiceImpl extends ServiceImpl<SysUserMapper, SysUser> implements SysUserService {

    private final PasswordEncoder passwordEncoder;

    @Override
    public SysUser getByEmpNo(String empNo) {
        return getOne(new LambdaQueryWrapper<SysUser>()
                .eq(SysUser::getEmpNo, empNo)
                .eq(SysUser::getStatus, 1)
                .eq(SysUser::getDeleted, 0));
    }

    @Override
    public SysUser getByWecomUserId(String wecomUserId) {
        return getOne(new LambdaQueryWrapper<SysUser>()
                .eq(SysUser::getWecomUserId, wecomUserId)
                .eq(SysUser::getStatus, 1)
                .eq(SysUser::getDeleted, 0));
    }

    @Override
    public boolean updatePassword(Long userId, String newPassword) {
        return update(new LambdaUpdateWrapper<SysUser>()
                .eq(SysUser::getId, userId)
                .set(SysUser::getPassword, passwordEncoder.encode(newPassword))
                .set(SysUser::getPasswordUpdatedAt, LocalDateTime.now()));
    }

    @Override
    public boolean updateLoginInfo(Long userId, String ip) {
        SysUser user = getById(userId);
        if (user == null) return false;
        user.setLastLoginAt(LocalDateTime.now());
        user.setLastLoginIp(ip);
        return updateById(user);
    }
}
