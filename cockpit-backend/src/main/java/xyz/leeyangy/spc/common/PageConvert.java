package xyz.leeyangy.spc.common;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 分页结果转换工具
 *
 * <p>MyBatis-Plus 的 {@code IPage.convert()} 返回 {@code IPage<R>} 而非 {@code Page<R>}，
 * 导致 {@code R<Page<XxxVO>>} 声明与 javac 类型推断不兼容。本工具方法构造新的
 * {@code Page<R>} 携带原分页信息（current/size/total）与转换后的 records。</p>
 */
public final class PageConvert {

    private PageConvert() {}

    public static <T, R> Page<R> convert(Page<T> page, Function<T, R> mapper) {
        Page<R> result = new Page<>(page.getCurrent(), page.getSize(), page.getTotal());
        List<R> records = page.getRecords().stream().map(mapper).collect(Collectors.toList());
        result.setRecords(records);
        return result;
    }
}
