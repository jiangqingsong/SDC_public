package com.broadtech.analyse.flink.process;

import com.broadtech.analyse.pojo.traffic.Traffic;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author jiangqingsong
 * @description
 * @date 2020-05-06 16:31
 */
public class TrafficProcessFunction extends ProcessWindowFunction<Traffic, String, String, TimeWindow> {
    @Override
    public void process(String s, Context context, Iterable<Traffic> elements, Collector<String> out) throws Exception {
        Traffic next = elements.iterator().next();
        out.collect(context.window().getEnd() + "\t" + next.getTotalTraffic() + "|" + next.getAbnormalTraffic() + "|" + next.getNormalTraffic());

    }
}
