package com.broadtech.analyse.flink.function;

import org.apache.flink.api.common.functions.MapFunction;

/**
 * @author jiangqingsong
 * @description
 * @date 2020-05-09 16:10
 */
public class GatewaySplitMapFn implements MapFunction<String, String[]> {
    @Override
    public String[] map(String value) throws Exception {
        return new String[0];
    }
}
