package xyz.leeyangy.spc.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.stereotype.Service;
import xyz.leeyangy.spc.entity.Equipment;
import xyz.leeyangy.spc.mapper.EquipmentMapper;
import xyz.leeyangy.spc.service.EquipmentService;

import java.util.List;

@Service
public class EquipmentServiceImpl extends ServiceImpl<EquipmentMapper, Equipment> implements EquipmentService {

    @Override
    public List<Equipment> listByProcessId(Long processId) {
        if (processId == null) return list(new LambdaQueryWrapper<Equipment>()
                .eq(Equipment::getDeleted, 0)
                .eq(Equipment::getStatus, "正常")
                .orderByAsc(Equipment::getEquipName));
        return list(new LambdaQueryWrapper<Equipment>()
                .eq(Equipment::getProcessId, processId)
                .eq(Equipment::getDeleted, 0)
                .eq(Equipment::getStatus, "正常")
                .orderByAsc(Equipment::getEquipName));
    }
}
