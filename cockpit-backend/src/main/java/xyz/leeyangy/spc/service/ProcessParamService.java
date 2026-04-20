package xyz.leeyangy.spc.service;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import xyz.leeyangy.spc.entity.ProcessParam;
import xyz.leeyangy.spc.mapper.ProcessParamMapper;

import java.util.List;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class ProcessParamService extends ServiceImpl<ProcessParamMapper, ProcessParam> {

    public List<Long> getParamIdsByProcessId(Long processId) {
        return list(new LambdaQueryWrapper<ProcessParam>()
                .eq(ProcessParam::getProcessId, processId)
                .orderByAsc(ProcessParam::getSortOrder))
                .stream()
                .map(ProcessParam::getParamId)
                .collect(Collectors.toList());
    }

    public List<ProcessParam> getBindingsByProcessId(Long processId) {
        return list(new LambdaQueryWrapper<ProcessParam>()
                .eq(ProcessParam::getProcessId, processId)
                .orderByAsc(ProcessParam::getSortOrder));
    }

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

    public boolean unbindParam(Long processId, Long paramId) {
        return baseMapper.physicalDelete(processId, paramId) > 0;
    }

    @lombok.Data
    public static class BindItem {
        private Long paramId;
    }
}
