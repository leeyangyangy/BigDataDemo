package xyz.leeyangy.spc.service;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.IService;
import xyz.leeyangy.spc.entity.Process;

import java.util.List;

/**
 * 工序 Service 接口
 */
public interface ProcessService extends IService<Process> {

    List<Process> listByProduct(Long productId, Long workshopId);

    Page<Process> page(Page<Process> page, Long productId, String keyword);
}
