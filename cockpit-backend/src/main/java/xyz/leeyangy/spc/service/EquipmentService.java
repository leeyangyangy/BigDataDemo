package xyz.leeyangy.spc.service;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import xyz.leeyangy.spc.entity.Equipment;
import xyz.leeyangy.spc.mapper.EquipmentMapper;

@Service
@RequiredArgsConstructor
public class EquipmentService extends ServiceImpl<EquipmentMapper, Equipment> {
}
