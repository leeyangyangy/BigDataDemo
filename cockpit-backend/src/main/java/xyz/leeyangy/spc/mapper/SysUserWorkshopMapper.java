package xyz.leeyangy.spc.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import xyz.leeyangy.spc.entity.SysUserWorkshop;

@Mapper
public interface SysUserWorkshopMapper extends BaseMapper<SysUserWorkshop> {

    /**
     * 物理删除用户某类型的所有绑定 (绕过逻辑删除, 避免唯一索引冲突)。
     * 重新绑定场景使用, 关联表无审计需求, 直接物理删除最干净。
     */
    @Delete("DELETE FROM sys_user_workshop WHERE user_id = #{userId} AND bind_type = #{bindType}")
    int physicalDeleteByUserAndType(@Param("userId") Long userId, @Param("bindType") String bindType);
}
