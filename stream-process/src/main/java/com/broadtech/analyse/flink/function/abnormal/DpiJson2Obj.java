package com.broadtech.analyse.flink.function.abnormal;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.broadtech.analyse.pojo.abnormal.Dpi;
import com.broadtech.analyse.pojo.ss.SecurityLog;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.log4j.Logger;

import java.util.UUID;

/**
 * @author leo.J
 * @description
 * 1、原始Json转Dpi
 * @date 2020-08-12 15:57
 */
public class DpiJson2Obj extends RichMapFunction<ObjectNode, Dpi> {
    private static Logger LOG = Logger.getLogger(DpiJson2Obj.class);

    @Override
    public Dpi map(ObjectNode log) throws Exception {
        SecurityLog securityLog;
        try {
            JSONObject dpiJson = JSON.parseObject(log.toString());
            String SrcIPAddress = dpiJson.get("SrcIPAddress").toString();
            String SrcPort = dpiJson.get("SrcPort").toString();
            String DestIPAddress = dpiJson.get("DestIPAddress").toString();
            String DestPort = dpiJson.get("DestPort").toString();
            String Protocol = dpiJson.get("Protocol").toString();
            String StartTime = dpiJson.get("StartTime").toString();
            String EndTime = dpiJson.get("EndTime").toString();
            String ConnectDuration = dpiJson.get("ConnectDuration").toString();
            String PacketSize = dpiJson.get("PacketSize").toString();
            String PacketNumber = dpiJson.get("PacketNumber").toString();
            String UpstreamTraffic = dpiJson.get("UpstreamTraffic").toString();
            String DownstreamTraffic = dpiJson.get("DownstreamTraffic").toString();
            String TrafficSize = dpiJson.get("TrafficSize").toString();
            String DomainName = dpiJson.get("DomainName").toString();
            String Url = dpiJson.get("Url").toString();
            String FileName = dpiJson.get("FileName").toString();
            String id = UUID.randomUUID().toString().replace("-", "").toLowerCase();

            Dpi dpi = new Dpi(SrcIPAddress, SrcPort, DestIPAddress, DestPort, Protocol, StartTime, EndTime, ConnectDuration,
                    PacketSize, PacketNumber, UpstreamTraffic, DownstreamTraffic, TrafficSize, DomainName, Url, FileName, id);
            return dpi;
        } catch (Exception e) {
            LOG.error("Parse origin dpi json error! Log contents: " + log.toString());
            return null;
        }
    }
}
