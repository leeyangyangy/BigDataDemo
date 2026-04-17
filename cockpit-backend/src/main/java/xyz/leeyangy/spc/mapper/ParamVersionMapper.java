package xyz.leeyangy.spc.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import xyz.leeyangy.spc.entity.ParamVersion;

@Mapper
public interface ParamVersionMapper extends BaseMapper<ParamVersion> {

    ParamVersion selectCurrentVersion(@Param("paramId") Long paramId, @Param("productId") Long productId);
}
