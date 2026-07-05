package xyz.leeyangy.spc.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import xyz.leeyangy.spc.entity.PasswordHistory;

import java.util.List;

@Mapper
public interface PasswordHistoryMapper extends BaseMapper<PasswordHistory> {

    /**
     * 查询用户最近 N 条密码历史 (按创建时间倒序)。
     */
    List<PasswordHistory> selectRecentByUserId(@Param("userId") Long userId,
                                                @Param("limit") int limit);

    /**
     * 删除用户超出保留数量之外的旧记录 (按创建时间倒序保留 N 条)。
     */
    int deleteOldRecords(@Param("userId") Long userId,
                          @Param("keepCount") int keepCount);
}
