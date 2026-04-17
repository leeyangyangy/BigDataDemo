package xyz.leeyangy.spc.service;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import xyz.leeyangy.spc.entity.ParamVersion;
import xyz.leeyangy.spc.mapper.ParamVersionMapper;

import java.time.LocalDateTime;
import java.util.List;

@Service
@RequiredArgsConstructor
public class ParamVersionService extends ServiceImpl<ParamVersionMapper, ParamVersion> {

    public ParamVersion getCurrentVersion(Long paramId, Long productId) {
        return baseMapper.selectCurrentVersion(paramId, productId);
    }

    public List<ParamVersion> getVersionHistory(Long paramId, Long productId) {
        return list(new LambdaQueryWrapper<ParamVersion>()
                .eq(ParamVersion::getParamId, paramId)
                .eq(ParamVersion::getProductId, productId)
                .eq(ParamVersion::getDeleted, 0)
                .orderByDesc(ParamVersion::getVersionNo));
    }

    public Page<ParamVersion> pageVersions(Page<ParamVersion> page, Long paramId, Long productId) {
        return page(page, new LambdaQueryWrapper<ParamVersion>()
                .eq(paramId != null, ParamVersion::getParamId, paramId)
                .eq(productId != null, ParamVersion::getProductId, productId)
                .eq(ParamVersion::getDeleted, 0)
                .orderByDesc(ParamVersion::getVersionNo));
    }

    public ParamVersion createNewVersion(ParamVersion newVersion) {
        ParamVersion current = getCurrentVersion(newVersion.getParamId(), newVersion.getProductId());

        int nextVersionNo = 1;
        Long prevVersionId = null;

        if (current != null) {
            nextVersionNo = current.getVersionNo() + 1;
            prevVersionId = current.getId();

            current.setIsCurrent(0);
            current.setEffectiveTo(LocalDateTime.now());
            updateById(current);
        }

        newVersion.setVersionNo(nextVersionNo);
        newVersion.setPrevVersionId(prevVersionId);
        newVersion.setIsCurrent(1);
        newVersion.setEffectiveFrom(LocalDateTime.now());
        newVersion.setEffectiveTo(null);

        save(newVersion);
        return newVersion;
    }
}
