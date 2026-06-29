package xyz.leeyangy.spc.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import xyz.leeyangy.spc.entity.Param;

@Mapper
public interface ParamMapper extends BaseMapper<Param> {

    /**
     * 统计指定 param_code 的记录数（含软删除）。
     * 使用 @Select 原生 SQL 绕过 @TableLogic，避免软删除记录导致命名冲突。
     * 注意：此处用全限定名 @org.apache.ibatis.annotations.Param 避免与实体类 Param 类名冲突。
     */
    @Select("SELECT COUNT(*) FROM spc_param WHERE param_code = #{code}")
    long countByCodeAll(@org.apache.ibatis.annotations.Param("code") String code);

    /**
     * 统计指定 param_name 的记录数（含软删除）。
     */
    @Select("SELECT COUNT(*) FROM spc_param WHERE param_name = #{name}")
    long countByNameAll(@org.apache.ibatis.annotations.Param("name") String name);
}
