package com.broadtech.analyse.flink.process;

import com.broadtech.analyse.pojo.traffic.Traffic;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author leo.J
 * @description
 * @date 2020-04-27 16:49
 */
public class TrafficResultProcessFunction implements WindowFunction<Traffic, String, Integer, TimeWindow> {
    @Override
    public void apply(Integer integer, TimeWindow window, Iterable<Traffic> input, Collector<String> out) throws Exception {
        Traffic next = input.iterator().next();
        out.collect(window.getEnd() + "\t" + next.getTotalTraffic() + "|" + next.getAbnormalTraffic() + "|" + next.getNormalTraffic());
    }
}
