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
 * @author jiangqingsong
 * @description
 * 1、原始Json转SecurityLog
 * @date 2020-08-12 15:57
 */
public class SecurityJson2SecurityLog extends RichMapFunction<ObjectNode, SecurityLog> {
    private static Logger LOG = Logger.getLogger(SecurityJson2SecurityLog.class);
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public SecurityLog map(ObjectNode log) throws Exception {
        SecurityLog securityLog;
        try {
            JSONObject logObj = JSON.parseObject(log.toString());
            String id = logObj.get("id").toString();
            String eventrecvtime = logObj.get("eventrecvtime").toString();
            String eventgeneratetime = logObj.get("eventgeneratetime").toString();
            String eventdurationtime = logObj.get("eventdurationtime").toString();
            String username = logObj.get("username").toString();
            String srcipaddress = logObj.get("srcipaddress").toString();
            String srcmacaddress = logObj.get("srcmacaddress").toString();
            String srcport = logObj.get("srcport").toString();
            String operation = logObj.get("operation").toString();
            String destipaddress = logObj.get("destipaddress").toString();
            String destmacaddress = logObj.get("destmacaddress").toString();
            String destport = logObj.get("destport").toString();
            String eventname = logObj.get("eventname").toString();
            String firsteventtype = logObj.get("firsteventtype").toString();
            String secondeventtype = logObj.get("secondeventtype").toString();
            String thirdeventtype = logObj.get("thirdeventtype").toString();
            String eventdesc = logObj.get("eventdesc").toString();
            String eventgrade = logObj.get("eventgrade").toString();
            String networkprotocal = logObj.get("networkprotocal").toString();
            String netappprotocal = logObj.get("netappprotocal").toString();
            String deviceipaddress = logObj.get("deviceipaddress").toString();
            String devicename = logObj.get("devicename").toString();
            String devicetype = logObj.get("devicetype").toString();
            String devicefactory = logObj.get("devicefactory").toString();
            String devicemodel = logObj.get("devicemodel").toString();
            String pid = logObj.get("pid").toString();
            String url = logObj.get("url").toString();
            String domain = logObj.get("domain").toString();
            String mail = logObj.get("mail").toString();
            String malwaresample = logObj.get("malwaresample").toString();
            String softwarename = logObj.get("softwarename").toString();
            String softwareversion = logObj.get("softwareversion").toString();

            securityLog = new SecurityLog(id,eventrecvtime,eventgeneratetime,eventdurationtime,username,srcipaddress,
                    srcmacaddress,srcport,operation,destipaddress,destmacaddress,destport,eventname,firsteventtype,
                    secondeventtype,thirdeventtype,eventdesc,eventgrade,networkprotocal,netappprotocal,deviceipaddress,
                    devicename,devicetype,devicefactory,devicemodel,pid,url,domain,mail,malwaresample,softwarename,softwareversion);

            return securityLog;
        } catch (Exception e) {
            LOG.error("Parse origin log error! Log contents: " + log.toString());
            return null;
        }
    }
}
