package xyz.leeyangy.spc.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import xyz.leeyangy.spc.entity.ParamVersion;

@Mapper
public interface ParamVersionMapper extends BaseMapper<ParamVersion> {

    ParamVersion selectCurrentVersion(@Param("paramId") Long paramId, @Param("productId") Long productId);

    /**
     * 查询指定参数+产品下(含软删除记录)的最大版本号, 用于创建新版本时避免唯一键冲突
     */
    Integer selectMaxVersionNo(@Param("paramId") Long paramId, @Param("productId") Long productId);
}
