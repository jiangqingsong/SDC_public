package com.broadtech.analyse.flink.process.test;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author leo.J
 * @description
 * @date 2020-05-14 10:14
 */
public class TempChangeAlert extends KeyedProcessFunction<Long, Tuple3<Long, Long, Double>, Tuple3<Long, Double, Double>> {

    private ValueState<Double> lastTempState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ValueStateDescriptor<Double> lastTempStateDesc = new ValueStateDescriptor<>("last-temp", Double.class);
        lastTempState = getRuntimeContext().getState(lastTempStateDesc);
    }

    @Override
    public void processElement(Tuple3<Long, Long, Double> value, Context ctx, Collector<Tuple3<Long, Double, Double>> out) throws Exception {
        Double lastTemp = lastTempState.value();
        Double diff = Math.abs(value.f2 - lastTemp);
        if(diff > 10){
            out.collect(new Tuple3<>(value.f0, lastTemp, value.f2));
        }
        lastTempState.update(value.f2 );
    }
}
