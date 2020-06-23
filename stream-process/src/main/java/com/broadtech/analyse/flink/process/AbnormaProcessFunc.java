package com.broadtech.analyse.flink.process;

import com.broadtech.analyse.pojo.cmcc.Vulnerability;
import com.broadtech.analyse.pojo.traffic.Traffic;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Map;

/**
 * @author jiangqingsong
 * @description
 * @date 2020-04-27 13:58
 */
public class AbnormaProcessFunc extends BroadcastProcessFunction<Tuple2<String, Double>, Map<Integer, Tuple2<Double, Double>>, Traffic> {
    MapStateDescriptor<Void, Map<Integer, Tuple2<Double, Double>>> configDescriptor;

    @Override
    public void open(Configuration parameters) throws Exception {
        configDescriptor = new MapStateDescriptor(
                "abnormalBound", Types.VOID, Types.MAP(Types.INT, Types.TUPLE(Types.DOUBLE, Types.DOUBLE)));
    }

    /**
     * 处理每条数据
     * @param value
     * @param ctx
     * @param out
     * @throws Exception
     */
    @Override
    public void processElement(Tuple2<String, Double> value, ReadOnlyContext ctx, Collector<Traffic> out) throws Exception {
        Double abnormalTraffic = 0.0;
        String startTime = value.f0;
        Integer hour = Integer.valueOf(startTime.substring(11, 13));
        Double totalTraffic = value.f1;
        ReadOnlyBroadcastState<Void, Map<Integer, Tuple2<Double, Double>>> broadcastState
                = ctx.getBroadcastState(configDescriptor);
        //System.out.println("==processElement==: " + broadcastState.get(null));
        Map<Integer, Tuple2<Double, Double>> configMap = broadcastState.get(null);
        Tuple2<Double, Double> trafficBound = configMap.get(hour);
        if(totalTraffic > trafficBound.f1){
            abnormalTraffic = totalTraffic - trafficBound.f1;
        }else if(totalTraffic < trafficBound.f0){
            abnormalTraffic = trafficBound.f0 - totalTraffic;
        }
        Traffic traffic = new Traffic();
        traffic.setTime(startTime);
        traffic.setTotalTraffic(totalTraffic);
        traffic.setAbnormalTraffic(abnormalTraffic);
        traffic.setNormalTraffic(totalTraffic-abnormalTraffic);
        System.out.println("==processElement==:" + totalTraffic + "|" + abnormalTraffic + "|" + (totalTraffic-abnormalTraffic));
        out.collect(traffic);
    }

    /**
     * update broadcast
     * @param value
     * @param ctx
     * @param out
     * @throws Exception
     */
    @Override
    public void processBroadcastElement(Map<Integer, Tuple2<Double, Double>> value, Context ctx, Collector<Traffic> out) throws Exception {
        BroadcastState<Void, Map<Integer, Tuple2<Double, Double>>> broadcastState = ctx.getBroadcastState(configDescriptor);
        //System.out.println("==processBroadcastElement==: " + broadcastState.get(null));
        broadcastState.clear();
        broadcastState.put(null, value);
    }
}
