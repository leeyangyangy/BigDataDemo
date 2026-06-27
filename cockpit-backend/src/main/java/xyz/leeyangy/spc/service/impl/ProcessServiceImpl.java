package xyz.leeyangy.spc.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import xyz.leeyangy.spc.entity.Process;
import xyz.leeyangy.spc.mapper.ProcessMapper;
import xyz.leeyangy.spc.service.ProcessService;
import xyz.leeyangy.spc.service.ProductProcessService;

import java.util.Collections;
import java.util.List;

@Service
@RequiredArgsConstructor
public class ProcessServiceImpl extends ServiceImpl<ProcessMapper, Process> implements ProcessService {

    private final ProductProcessService productProcessService;

    @Override
    public List<Process> listByProduct(Long productId, Long workshopId) {
        if (productId != null) {
            List<Long> processIds = productProcessService.getProcessIdsByProductId(productId);
            if (processIds.isEmpty()) {
                return Collections.emptyList();
            }
            LambdaQueryWrapper<Process> wrapper = new LambdaQueryWrapper<Process>()
                    .in(Process::getId, processIds)
                    .eq(Process::getDeleted, 0)
                    .eq(Process::getStatus, 1);
            if (workshopId != null) {
                wrapper.eq(Process::getWorkshopId, workshopId);
            }
            wrapper.orderByAsc(Process::getProcessName);
            return list(wrapper);
        } else {
            LambdaQueryWrapper<Process> wrapper = new LambdaQueryWrapper<Process>()
                    .eq(Process::getDeleted, 0)
                    .eq(Process::getStatus, 1)
                    .orderByAsc(Process::getProcessName);
            if (workshopId != null) {
                wrapper.eq(Process::getWorkshopId, workshopId);
            }
            return list(wrapper);
        }
    }

    @Override
    public Page<Process> page(Page<Process> page, Long productId, String keyword) {
        LambdaQueryWrapper<Process> wrapper = new LambdaQueryWrapper<Process>()
                .eq(Process::getDeleted, 0);

        if (productId != null) {
            List<Long> processIds = productProcessService.getProcessIdsByProductId(productId);
            if (!processIds.isEmpty()) {
                wrapper.in(Process::getId, processIds);
            } else {
                wrapper.eq(Process::getId, -1L);
            }
        }

        if (StringUtils.hasText(keyword)) {
            wrapper.and(w -> w
                    .like(Process::getProcessCode, keyword)
                    .or()
                    .like(Process::getProcessName, keyword));
        }

        wrapper.orderByDesc(Process::getCreatedAt);
        return page(page, wrapper);
    }
}
