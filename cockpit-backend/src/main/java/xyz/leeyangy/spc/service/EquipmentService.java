package xyz.leeyangy.spc.service;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import xyz.leeyangy.spc.entity.Equipment;
import xyz.leeyangy.spc.mapper.EquipmentMapper;

import java.util.List;

@Service
@RequiredArgsConstructor
public class EquipmentService extends ServiceImpl<EquipmentMapper, Equipment> {

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