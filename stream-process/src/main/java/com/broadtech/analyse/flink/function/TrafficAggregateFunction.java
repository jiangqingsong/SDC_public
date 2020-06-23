package com.broadtech.analyse.flink.function;

import com.broadtech.analyse.pojo.traffic.Traffic;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * @author jiangqingsong
 * @description
 * @date 2020-04-27 16:39
 */
public class TrafficAggregateFunction implements AggregateFunction<Traffic, Traffic, Traffic> {
    @Override
    public Traffic createAccumulator() {
        Traffic traffic = new Traffic();
        traffic.setAbnormalTraffic(0.0);
        traffic.setNormalTraffic(0.0);
        traffic.setTotalTraffic(0.0);
        return traffic;
    }

    @Override
    public Traffic add(Traffic value, Traffic acc) {
        //对totalTraffic、normalTraffic、abnormalTraffic
        acc.setTotalTraffic(acc.getTotalTraffic() + value.getTotalTraffic());
        acc.setNormalTraffic(acc.getNormalTraffic() + value.getNormalTraffic());
        acc.setAbnormalTraffic(acc.getAbnormalTraffic() + value.getAbnormalTraffic());
        return acc;
    }

    @Override
    public Traffic getResult(Traffic accumulator) {
        return accumulator;
    }

    @Override
    public Traffic merge(Traffic a, Traffic b) {
        a.setTotalTraffic(a.getTotalTraffic() + b.getTotalTraffic());
        a.setNormalTraffic(a.getNormalTraffic() + b.getNormalTraffic());
        a.setAbnormalTraffic(a.getAbnormalTraffic() + b.getAbnormalTraffic());
        return a;
    }
}
