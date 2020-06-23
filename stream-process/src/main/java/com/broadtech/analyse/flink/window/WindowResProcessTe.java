package com.broadtech.analyse.flink.window;

import com.broadtech.analyse.pojo.user.ItemViewCount;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author jiangqingsong
 * @description
 * @date 2020-05-11 15:45
 */
public class WindowResProcessTe implements WindowFunction<Long, ItemViewCount, Long, TimeWindow> {
    @Override
    public void apply(Long itemId, TimeWindow window, Iterable<Long> agg, Collector<ItemViewCount> out) throws Exception {
        Long count = agg.iterator().next();
        out.collect(new ItemViewCount(itemId, window.getEnd(), count));
    }
}


