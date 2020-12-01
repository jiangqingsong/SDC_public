package com.broadtech.analyse.flink.process.ss;

import com.broadtech.analyse.pojo.ss.SecurityLog;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * @author leo.J
 * @description
 * @date 2020-08-12 16:45
 */
public class ComputeSecurityEvenCountFunc extends ProcessWindowFunction<SecurityLog, Tuple3<SecurityLog, Integer, Long>, Integer, TimeWindow> {
    private static Logger LOG = Logger.getLogger(ComputeSecurityEvenCountFunc.class);

    @Override
    public void process(Integer key, Context context,
                        Iterable<SecurityLog> elements, Collector<Tuple3<SecurityLog, Integer, Long>> out) throws Exception {
        Map<String, List<SecurityLog>> distinctLogMap = new HashMap<>();
        Map<String, Integer> eventCountMap = new HashMap<>();
        Iterator<SecurityLog> iterator = elements.iterator();
        int windowSize = 0;
        while (iterator.hasNext()) {

            SecurityLog currentSecurity = iterator.next();
            String eventKey = getEventKey(currentSecurity);
            //将相同事件数据存入distinctLogMap
            if(distinctLogMap.containsKey(eventKey)){
                List<SecurityLog> securityLogs = distinctLogMap.get(eventKey);
                securityLogs.add(currentSecurity);
                distinctLogMap.put(eventKey, securityLogs);
            }else{
                List<SecurityLog> securityLogs = new ArrayList<>();
                securityLogs.add(currentSecurity);
                distinctLogMap.put(eventKey, securityLogs);
            }

            windowSize++;

            if (!eventCountMap.containsKey(eventKey)) {
                eventCountMap.put(eventKey, 1);
            } else {
                Integer count = eventCountMap.get(eventKey);
                eventCountMap.put(eventKey, count + 1);
            }
        }
        Set<String> keySet = distinctLogMap.keySet();
        for(String key1: keySet){
            List<SecurityLog> securityLogs = distinctLogMap.get(key1);
            SecurityLog securityLog = securityLogs.get(securityLogs.size() - 1);
            out.collect(Tuple3.of(securityLog, eventCountMap.get(key1), context.window().getEnd()));
        }
        System.out.println("windowSize: " + windowSize);
    }

    public String getEventKey(SecurityLog securityLog){
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
        String eventKey = distinctKey.toString();
        return eventKey;
    }

}
