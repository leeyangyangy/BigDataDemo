package xyz.leeyangy.spc.controller;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import xyz.leeyangy.spc.common.R;
import xyz.leeyangy.spc.entity.Process;
import xyz.leeyangy.spc.service.ProcessService;

@RestController
@RequestMapping("/api/spc/process")
@RequiredArgsConstructor
public class ProcessController {

    private final ProcessService processService;

    @GetMapping("/page")
    public R<Page<Process>> page(
            @RequestParam(defaultValue = "1") Integer current,
            @RequestParam(defaultValue = "20") Integer size) {
        return R.ok(processService.page(new Page<>(current, size)));
    }

    @PostMapping
    public R<Process> create(@RequestBody Process process) {
        processService.save(process);
        return R.ok(process);
    }

    @GetMapping("/{id}")
    public R<Process> getById(@PathVariable Long id) {
        return R.ok(processService.getById(id));
    }
}
