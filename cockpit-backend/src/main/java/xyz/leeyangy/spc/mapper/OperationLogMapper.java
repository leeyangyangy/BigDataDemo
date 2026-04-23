package xyz.leeyangy.spc.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;
import xyz.leeyangy.spc.entity.OperationLog;

@Mapper
public interface OperationLogMapper extends BaseMapper<OperationLog> {
}
