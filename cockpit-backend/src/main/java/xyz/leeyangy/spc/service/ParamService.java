package xyz.leeyangy.spc.service;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import xyz.leeyangy.spc.entity.Param;
import xyz.leeyangy.spc.mapper.ParamMapper;

@Service
@RequiredArgsConstructor
public class ParamService extends ServiceImpl<ParamMapper, Param> {
}
