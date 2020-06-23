package com.broadtech.analyse.flink.window;

import com.broadtech.analyse.pojo.gateway.GatewayAppTypeView;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author jiangqingsong
 * @description
 * @date 2020-05-11 16:46
 */
public class GatewayAppTypeResProcessFn implements WindowFunction<Double, GatewayAppTypeView, String, TimeWindow> {
    @Override
    public void apply(String key, TimeWindow window, Iterable<Double> aggr, Collector<GatewayAppTypeView> out) throws Exception {
        String appType = key;
        Double allTraffic = aggr.iterator().next();
        out.collect(new GatewayAppTypeView(appType, allTraffic, window.getEnd()));
    }
}
