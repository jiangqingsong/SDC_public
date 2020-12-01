package com.broadtech.analyse.flink.function.ss;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.broadtech.analyse.pojo.ss.SecurityLog;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * @author leo.J
 * @description
 * 1、原始Json转SecurityLog
 * 2、增加字段eventCount
 * @date 2020-08-04 15:57
 */
public class SecurityJson2Obj extends RichMapFunction<ObjectNode, Tuple2<SecurityLog, Long>> {
    private static Logger LOG = Logger.getLogger(SecurityJson2Obj.class);
    private transient ValueState<Map<String, Long>> eventCountState;
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ValueStateDescriptor descriptor = new ValueStateDescriptor<>("eventCountState", Types.MAP(Types.STRING, Types.LONG));
        eventCountState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public Tuple2<SecurityLog, Long> map(ObjectNode log) throws Exception {
        SecurityLog securityLog;
        try {
            JSONObject logObj = JSON.parseObject(log.toString());
            String id = logObj.get("id").toString();
            String eventname = logObj.get("eventname").toString();
            String firsteventtype = logObj.get("firsteventtype").toString();
            String secondeventtype = logObj.get("secondeventtype").toString();
            String thirdeventtype = logObj.get("thirdeventtype").toString();
            String eventgrade = logObj.get("eventgrade").toString();
            String discoverytime = logObj.get("discoverytime").toString();
            String alarmtimes = logObj.get("alarmtimes").toString();
            String deviceipaddress = logObj.get("deviceipaddress").toString();
            String devicetype = logObj.get("devicetype").toString();
            String devicefactory = logObj.get("devicefactory").toString();
            String devicemodel = logObj.get("devicemodel").toString();
            String devicename = logObj.get("devicename").toString();
            String srcipaddress = logObj.get("srcipaddress").toString();
            String srcport = logObj.get("srcport").toString();
            String srcmacaddress = logObj.get("srcmacaddress").toString();
            String destipaddress = logObj.get("destipaddress").toString();
            String destport = logObj.get("destport").toString();
            String affectdevice = logObj.get("affectdevice").toString();
            String eventdesc = logObj.get("eventdesc").toString();
            /*securityLog = new SecurityLog(id,eventname,firsteventtype,secondeventtype,thirdeventtype,eventgrade,discoverytime,
                    alarmtimes,deviceipaddress,devicetype,devicefactory,devicemodel,devicename,srcipaddress,srcport,srcmacaddress,
                    destipaddress,destport,affectdevice,eventdesc);*/
            securityLog = null;
            //count //todo  这里的distinctKey要再次确定下
            Map<String, Long> countMap = eventCountState.value();
            StringBuilder distinctKey = new StringBuilder();
            distinctKey.append(deviceipaddress);
            distinctKey.append(eventname);
            distinctKey.append(firsteventtype);
            distinctKey.append(secondeventtype);
            distinctKey.append(thirdeventtype);
            Long eventCount = 0L;//todo  这里是统计的count
            if(!countMap.containsKey(distinctKey.toString())){
                Map<String, Long> map = new HashMap<String, Long>();
                map.put(distinctKey.toString(), 1L);
                eventCountState.update(map);
                eventCount = 1L;
            }else{
                Long count = countMap.get(distinctKey);
                countMap.put(distinctKey.toString(), count+1);
                eventCount = count+1;
                eventCountState.update(countMap);
            }
            return Tuple2.of(securityLog, eventCount);
        } catch (Exception e) {
            LOG.error("Parse origin log error! Log contents: " + log.toString());
            return null;
        }
    }
}
