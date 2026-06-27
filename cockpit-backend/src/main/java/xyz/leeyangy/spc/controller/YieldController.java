package xyz.leeyangy.spc.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import xyz.leeyangy.spc.common.R;
import xyz.leeyangy.spc.service.YieldService;
import xyz.leeyangy.spc.vo.YieldDataVO;

import java.util.List;

/**
 * 良率监控接口
 */
@Slf4j
@RestController
@RequestMapping("/api/yield")
@RequiredArgsConstructor
public class YieldController {

    private final YieldService yieldService;

    /**
     * 获取良率数据（可按车间、日期范围筛选）
     */
    @GetMapping("/data")
    public R<YieldDataVO> getData(
            @RequestParam(required = false) String workshop,
            @RequestParam(required = false, name = "start_date") String startDate,
            @RequestParam(required = false, name = "end_date") String endDate) {
        return R.ok(yieldService.getYieldData(workshop, startDate, endDate));
    }

    /**
     * 搜索良率数据
     */
    @GetMapping("/search")
    public R<YieldDataVO> search(
            @RequestParam(required = false) String workshop,
            @RequestParam(required = false) String keyword,
            @RequestParam(required = false, name = "start_date") String startDate,
            @RequestParam(required = false, name = "end_date") String endDate) {
        return R.ok(yieldService.searchYieldData(workshop, keyword, startDate, endDate));
    }

    /**
     * 获取可用车间列表
     */
    @GetMapping("/workshops")
    public R<List<String>> workshops() {
        return R.ok(yieldService.getWorkshops());
    }
}
