package xyz.leeyangy.spc.service;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.IService;
import xyz.leeyangy.spc.entity.ParamVersion;

import java.util.List;

/**
 * 参数版本 Service 接口
 */
public interface ParamVersionService extends IService<ParamVersion> {

    ParamVersion getCurrentVersion(Long paramId, Long productId);

    ParamVersion getAnyCurrentVersion(Long paramId);

    List<ParamVersion> getVersionHistory(Long paramId, Long productId);

    Page<ParamVersion> pageVersions(Page<ParamVersion> page, Long paramId, Long productId);

    ParamVersion createNewVersion(ParamVersion newVersion);

    boolean enableVersion(Long id);

    boolean disableVersion(Long id);

    boolean updateVersion(Long id, ParamVersion updated);

    boolean deleteVersion(Long id);
}
