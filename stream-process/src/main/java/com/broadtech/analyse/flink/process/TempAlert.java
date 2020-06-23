package com.broadtech.analyse.flink.process;

import com.broadtech.analyse.pojo.gateway.GatewaySession;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author jiangqingsong
 * @description
 * @date 2020-05-11 9:53
 */
public class TempAlert extends KeyedProcessFunction<String, GatewaySession, String> {

    
    @Override
    public void processElement(GatewaySession value, Context ctx, Collector<String> out) throws Exception {

    }
}
