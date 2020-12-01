package com.broadtech.analyse.flink.process;

import com.broadtech.analyse.pojo.user.UserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * @author leo.J
 * @leo.J
 * @date 2020-05-11 15:42
 */
public class CountAgg implements AggregateFunction<UserBehavior, Long, Long> {
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(UserBehavior value, Long accumulator) {
        return accumulator + 1;
    }

    @Override
    public Long getResult(Long accumulator) {
        return accumulator;
    }

    @Override
    public Long merge(Long a, Long b) {
        return a + b;
    }
}
