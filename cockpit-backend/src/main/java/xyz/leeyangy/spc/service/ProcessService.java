package xyz.leeyangy.spc.service;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import xyz.leeyangy.spc.entity.Process;
import xyz.leeyangy.spc.mapper.ProcessMapper;

@Service
@RequiredArgsConstructor
public class ProcessService extends ServiceImpl<ProcessMapper, Process> {
}
