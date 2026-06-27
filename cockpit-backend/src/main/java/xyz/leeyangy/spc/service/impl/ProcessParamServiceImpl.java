package xyz.leeyangy.spc.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import xyz.leeyangy.spc.entity.Param;
import xyz.leeyangy.spc.entity.ProcessParam;
import xyz.leeyangy.spc.mapper.ProcessParamMapper;
import xyz.leeyangy.spc.service.ParamService;
import xyz.leeyangy.spc.service.ProcessParamService;
import xyz.leeyangy.spc.vo.ParamSimpleVO;

import java.util.List;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class ProcessParamServiceImpl extends ServiceImpl<ProcessParamMapper, ProcessParam> implements ProcessParamService {

    private final ParamService paramService;

    @Override
    public List<Long> getParamIdsByProcessId(Long processId) {
        return list(new LambdaQueryWrapper<ProcessParam>()
                .eq(ProcessParam::getProcessId, processId)
                .orderByAsc(ProcessParam::getSortOrder))
                .stream()
                .map(ProcessParam::getParamId)
                .collect(Collectors.toList());
    }

    @Override
    public List<ParamSimpleVO> getProcessParams(Long processId) {
        List<Long> paramIds = getParamIdsByProcessId(processId);
        if (paramIds.isEmpty()) {
            return List.of();
        }
        List<Param> params = paramService.listByIds(paramIds);
        return params.stream().map(param -> {
            ParamSimpleVO vo = new ParamSimpleVO();
            vo.setParamId(param.getId());
            vo.setParamCode(param.getParamCode());
            vo.setParamName(param.getParamName());
            return vo;
        }).collect(Collectors.toList());
    }

    @Override
    public boolean bindParams(Long processId, List<BindItem> items) {
        baseMapper.physicalDeleteByProcessId(processId);
        if (items == null || items.isEmpty()) return true;
        int sort = 0;
        for (BindItem item : items) {
            ProcessParam pp = new ProcessParam();
            pp.setProcessId(processId);
            pp.setParamId(item.getParamId());
            pp.setSortOrder(sort++);
            save(pp);
        }
        return true;
    }

    @Override
    public boolean bindParam(Long processId, Long paramId) {
        long count = count(new LambdaQueryWrapper<ProcessParam>()
                .eq(ProcessParam::getProcessId, processId)
                .eq(ProcessParam::getParamId, paramId));
        if (count > 0) return true;
        ProcessParam pp = new ProcessParam();
        pp.setProcessId(processId);
        pp.setParamId(paramId);
        pp.setSortOrder((int) count(new LambdaQueryWrapper<ProcessParam>().eq(ProcessParam::getProcessId, processId)));
        return save(pp);
    }

    @Override
    public boolean unbindParam(Long processId, Long paramId) {
        return baseMapper.physicalDelete(processId, paramId) > 0;
    }
}
