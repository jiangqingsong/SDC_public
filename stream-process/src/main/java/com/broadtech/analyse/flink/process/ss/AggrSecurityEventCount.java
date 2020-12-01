package com.broadtech.analyse.flink.process.ss;

import com.broadtech.analyse.pojo.ss.SecurityLog;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * @author leo.J
 * @description
 * @date 2020-08-17 15:15
 */
public class AggrSecurityEventCount implements AggregateFunction<SecurityLog, Integer, Integer> {
    @Override
    public Integer createAccumulator() {
        return 0;
    }

    @Override
    public Integer add(SecurityLog value, Integer accumulator) {
        return accumulator + 1;
    }

    @Override
    public Integer getResult(Integer accumulator) {
        return accumulator;
    }

    @Override
    public Integer merge(Integer a, Integer b) {
        return a + b;
    }
}
