package com.broadtech.analyse.flink.process.ss;

import com.broadtech.analyse.pojo.ss.SecurityLog;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * @author leo.J
 * @description
 * @date 2020-08-17 16:03
 */
public class ComputeSecurityEvenCountKeyedFunc extends KeyedProcessFunction<Tuple5<String, String, String, String, String>, SecurityLog, Tuple2<SecurityLog, Integer>> {
    private static Logger LOG = Logger.getLogger(ComputeSecurityEvenCountKeyedFunc.class);
    private transient ValueState<Map<String, Integer>> eventCountState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ValueStateDescriptor descriptor = new ValueStateDescriptor<>("eventCountState", Types.MAP(Types.STRING, Types.INT));
        eventCountState = getRuntimeContext().getState(descriptor);
        eventCountState.update(new HashMap<String, Integer>());
    }

    @Override
    public void processElement(SecurityLog securityLog, Context ctx, Collector<Tuple2<SecurityLog, Integer>> out) throws Exception {
        Map<String, Integer> countMap = eventCountState.value();

        StringBuilder distinctKey = new StringBuilder();
        String deviceipaddress = securityLog.getDeviceipaddress();
        String eventname = securityLog.getEventname();
        String firsteventtype = securityLog.getFirsteventtype();
        String secondeventtype = securityLog.getSecondeventtype();
        String thirdeventtype = securityLog.getThirdeventtype();
        distinctKey.append(deviceipaddress);
        distinctKey.append(eventname);
        distinctKey.append(firsteventtype);
        distinctKey.append(secondeventtype);
        distinctKey.append(thirdeventtype);
        Integer eventCount = 0;//todo  这里是统计的count
        if (!countMap.containsKey(distinctKey.toString())) {
            Map<String, Integer> map = new HashMap<>();
            map.put(distinctKey.toString(), 1);
            eventCountState.update(map);
            eventCount = 1;
        } else {
            Integer count = countMap.get(distinctKey);
            countMap.put(distinctKey.toString(), count + 1);
            eventCount = count + 1;
            eventCountState.update(countMap);
        }
        out.collect(Tuple2.of(securityLog, eventCount));
    }
}
