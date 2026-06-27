package xyz.leeyangy.spc.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.stereotype.Service;
import xyz.leeyangy.spc.entity.Workshop;
import xyz.leeyangy.spc.mapper.WorkshopMapper;
import xyz.leeyangy.spc.service.WorkshopService;

@Service
public class WorkshopServiceImpl extends ServiceImpl<WorkshopMapper, Workshop> implements WorkshopService {
}
