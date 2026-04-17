package xyz.leeyangy.spc.service;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import xyz.leeyangy.spc.entity.Workshop;
import xyz.leeyangy.spc.mapper.WorkshopMapper;

@Service
@RequiredArgsConstructor
public class WorkshopService extends ServiceImpl<WorkshopMapper, Workshop> {
}