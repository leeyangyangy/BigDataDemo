package xyz.leeyangy.spc.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import xyz.leeyangy.spc.entity.Process;

@Mapper
public interface ProcessMapper extends BaseMapper<Process> {

    /**
     * 统计指定 process_code 的记录数（含软删除）。
     * 使用 @Select 原生 SQL 绕过 @TableLogic，避免软删除记录导致命名冲突。
     */
    @Select("SELECT COUNT(*) FROM spc_process WHERE process_code = #{code}")
    long countByCodeAll(@Param("code") String code);

    /**
     * 统计指定 process_name 的记录数（含软删除）。
     */
    @Select("SELECT COUNT(*) FROM spc_process WHERE process_name = #{name}")
    long countByNameAll(@Param("name") String name);
}
