package xyz.leeyangy.spc.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import xyz.leeyangy.spc.entity.SpcData;
import xyz.leeyangy.spc.vo.SpcDataDetailVO;

import java.time.LocalDateTime;
import java.util.Collection;

@Mapper
public interface SpcDataMapper extends BaseMapper<SpcData> {

    /**
     * 后台分页联查：spc_data 关联产品/工序/参数/设备/参数版本，
     * 一次性带出编码、名称、单位、版本号、规格/控制限等展示字段。
     *
     * <p>分页由 MyBatis-Plus 拦截器自动注入，XML 中无需写 LIMIT。
     *
     * @param processIds 工序ID集合（车间可见性过滤）；{@code null} 表示不过滤，
     *                   非空时仅返回这些工序下的数据，用于修复 IDOR 越权访问
     */
    IPage<SpcDataDetailVO> selectDetailPage(
            IPage<SpcDataDetailVO> page,
            @Param("paramVersionId") Long paramVersionId,
            @Param("batchId") String batchId,
            @Param("productId") Long productId,
            @Param("paramId") Long paramId,
            @Param("processId") Long processId,
            @Param("equipmentId") Long equipmentId,
            @Param("isOoc") Integer isOoc,
            @Param("isOos") Integer isOos,
            @Param("dataSource") String dataSource,
            @Param("startTime") LocalDateTime startTime,
            @Param("endTime") LocalDateTime endTime,
            @Param("processIds") Collection<Long> processIds);
}
