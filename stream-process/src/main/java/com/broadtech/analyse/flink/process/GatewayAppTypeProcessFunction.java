package com.broadtech.analyse.flink.process;

import com.broadtech.analyse.pojo.gateway.GatewayUserAction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author jiangqingsong
 * @description
 * @date 2020-05-06 16:31
 */
public class GatewayAppTypeProcessFunction extends ProcessWindowFunction<GatewayUserAction, String, String, TimeWindow> {
    @Override
    public void process(String s, Context context, Iterable<GatewayUserAction> elements, Collector<String> out) throws Exception {
        GatewayUserAction next = elements.iterator().next();
        out.collect(context.window().getEnd() + "\t" + next.getTotalTraffic());

    }
}
