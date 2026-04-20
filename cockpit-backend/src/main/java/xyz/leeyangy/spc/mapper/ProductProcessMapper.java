package xyz.leeyangy.spc.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import xyz.leeyangy.spc.entity.ProductProcess;

@Mapper
public interface ProductProcessMapper extends BaseMapper<ProductProcess> {

    @Delete("DELETE FROM spc_product_process WHERE product_id = #{productId}")
    int physicalDeleteByProductId(@Param("productId") Long productId);

    @Delete("DELETE FROM spc_product_process WHERE product_id = #{productId} AND process_id = #{processId}")
    int physicalDelete(@Param("productId") Long productId, @Param("processId") Long processId);
}
