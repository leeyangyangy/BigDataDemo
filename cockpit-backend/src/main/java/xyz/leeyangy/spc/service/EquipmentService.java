package xyz.leeyangy.spc.service;

import com.baomidou.mybatisplus.extension.service.IService;
import xyz.leeyangy.spc.entity.Equipment;

import java.util.List;

/**
 * 设备 Service 接口
 */
public interface EquipmentService extends IService<Equipment> {

    List<Equipment> listByProcessId(Long processId);
}
