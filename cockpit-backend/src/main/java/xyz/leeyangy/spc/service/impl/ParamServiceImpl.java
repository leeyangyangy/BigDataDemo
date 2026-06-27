package xyz.leeyangy.spc.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.stereotype.Service;
import xyz.leeyangy.spc.entity.Param;
import xyz.leeyangy.spc.mapper.ParamMapper;
import xyz.leeyangy.spc.service.ParamService;

@Service
public class ParamServiceImpl extends ServiceImpl<ParamMapper, Param> implements ParamService {
}
