package com.broadtech.analyse.flink.function;

import com.broadtech.analyse.pojo.gateway.GatewaySession;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * @author leo.J
 * @description
 * @date 2020-05-09 17:16
 */
public class GatewayAppTypeTrafficAggFn implements AggregateFunction<GatewaySession, Double, Double> {
    @Override
    public Double createAccumulator() {
        return 0.0;
    }

    @Override
    public Double add(GatewaySession value, Double accumulator) {
        return accumulator + value.getDlData() + value.getUlData();
    }

    @Override
    public Double getResult(Double accumulator) {
        return accumulator;
    }

    @Override
    public Double merge(Double a, Double b) {
        return a + b;
    }
}
