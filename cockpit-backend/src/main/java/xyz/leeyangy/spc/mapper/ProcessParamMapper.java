package xyz.leeyangy.spc.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import xyz.leeyangy.spc.entity.ProcessParam;

@Mapper
public interface ProcessParamMapper extends BaseMapper<ProcessParam> {

    @Delete("DELETE FROM spc_process_param WHERE process_id = #{processId}")
    int physicalDeleteByProcessId(@Param("processId") Long processId);

    @Delete("DELETE FROM spc_process_param WHERE process_id = #{processId} AND param_id = #{paramId}")
    int physicalDelete(@Param("processId") Long processId, @Param("paramId") Long paramId);
}
