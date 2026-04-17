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
        ParamVersion version = baseMapper.selectCurrentVersion(paramId, productId);
        if (version != null) return version;
        if (productId != null && productId > 0) {
            return baseMapper.selectCurrentVersion(paramId, 0L);
        }
        return null;
    }

    public ParamVersion getAnyCurrentVersion(Long paramId) {
        ParamVersion version = getOne(new LambdaQueryWrapper<ParamVersion>()
                .eq(ParamVersion::getParamId, paramId)
                .eq(ParamVersion::getIsCurrent, 1)
                .eq(ParamVersion::getStatus, 1)
                .eq(ParamVersion::getDeleted, 0)
                .ne(ParamVersion::getProductId, 0)
                .orderByDesc(ParamVersion::getVersionNo)
                .last("LIMIT 1"));
        if (version != null) return version;
        return getOne(new LambdaQueryWrapper<ParamVersion>()
                .eq(ParamVersion::getParamId, paramId)
                .eq(ParamVersion::getIsCurrent, 1)
                .eq(ParamVersion::getStatus, 1)
                .eq(ParamVersion::getDeleted, 0)
                .eq(ParamVersion::getProductId, 0)
                .orderByDesc(ParamVersion::getVersionNo)
                .last("LIMIT 1"));
    }

    public List<ParamVersion> getVersionHistory(Long paramId, Long productId) {
        LambdaQueryWrapper<ParamVersion> wrapper = new LambdaQueryWrapper<ParamVersion>()
                .eq(ParamVersion::getParamId, paramId)
                .eq(ParamVersion::getDeleted, 0)
                .orderByDesc(ParamVersion::getVersionNo);
        if (productId != null) {
            wrapper.eq(ParamVersion::getProductId, productId);
        }
        return list(wrapper);
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
        newVersion.setStatus(1);
        if (newVersion.getProductId() == null) {
            newVersion.setProductId(0L);
        }
        newVersion.setEffectiveFrom(LocalDateTime.now());
        newVersion.setEffectiveTo(null);

        save(newVersion);
        return newVersion;
    }

    public boolean enableVersion(Long id) {
        ParamVersion version = getById(id);
        if (version == null) throw new RuntimeException("版本不存在: " + id);
        version.setStatus(1);
        version.setIsCurrent(1);

        List<ParamVersion> siblings = list(new LambdaQueryWrapper<ParamVersion>()
                .eq(ParamVersion::getParamId, version.getParamId())
                .ne(ParamVersion::getId, id)
                .eq(ParamVersion::getDeleted, 0));
        for (ParamVersion sib : siblings) {
            if (sib.getStatus() != null && sib.getStatus() == 1) {
                sib.setStatus(0);
            }
            if (sib.getIsCurrent() != null && sib.getIsCurrent() == 1) {
                sib.setIsCurrent(0);
            }
            updateById(sib);
        }

        return updateById(version);
    }

    public boolean disableVersion(Long id) {
        ParamVersion version = getById(id);
        if (version == null) throw new RuntimeException("版本不存在: " + id);
        version.setStatus(0);
        if (version.getIsCurrent() != null && version.getIsCurrent() == 1) {
            version.setIsCurrent(0);
        }
        return updateById(version);
    }

    public boolean updateVersion(Long id, ParamVersion updated) {
        ParamVersion existing = getById(id);
        if (existing == null) throw new RuntimeException("版本不存在: " + id);
        existing.setUsl(updated.getUsl());
        existing.setLsl(updated.getLsl());
        existing.setTarget(updated.getTarget());
        existing.setUcl(updated.getUcl());
        existing.setLcl(updated.getLcl());
        existing.setCl(updated.getCl());
        existing.setSigmaWidth(updated.getSigmaWidth());
        existing.setSubgroupSize(updated.getSubgroupSize());
        existing.setChartType(updated.getChartType());
        return updateById(existing);
    }

    public boolean deleteVersion(Long id) {
        ParamVersion version = getById(id);
        if (version == null) throw new RuntimeException("版本不存在: " + id);
        if (version.getIsCurrent() != null && version.getIsCurrent() == 1) {
            throw new RuntimeException("当前生效的版本不能删除，请先切换到其他版本");
        }
        return removeById(id);
    }
}
