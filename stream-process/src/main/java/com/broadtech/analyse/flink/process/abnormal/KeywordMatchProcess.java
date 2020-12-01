package com.broadtech.analyse.flink.process.abnormal;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.broadtech.analyse.constants.abnormal.AlarmType;
import com.broadtech.analyse.constants.ss.ThreatIntelligenceType;
import com.broadtech.analyse.flink.source.ss.IntelligenceLibReader;
import com.broadtech.analyse.pojo.abnormal.AlarmResult;
import com.broadtech.analyse.pojo.abnormal.Dpi;
import com.broadtech.analyse.pojo.ss.SecurityLog;
import com.broadtech.analyse.pojo.ss.ThreatIntelligence;
import com.broadtech.analyse.pojo.ss.ThreatIntelligence2;
import com.broadtech.analyse.util.TimeUtils;
import com.mysql.cj.jdbc.Driver;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author leo.J
 * @description 关键字匹配
 * @date 2020-08-21 14:46
 */
public class KeywordMatchProcess extends ProcessFunction<Dpi, AlarmResult> {

    private static final Logger LOG = LoggerFactory.getLogger(KeywordMatchProcess.class);

    private Connection connection = null;
    private PreparedStatement ps1 = null;
    private PreparedStatement ps2 = null;
    private volatile boolean isRunning = true;

    //todo  7种威胁情报map
    private Map<String, ThreatIntelligence> ip4Map = new HashMap<>();
    private Map<String, ThreatIntelligence> domainMap = new HashMap<>();
    private Map<String, ThreatIntelligence> urlMap = new HashMap<>();
    private Map<String, ThreatIntelligence> hashMd5Map = new HashMap<>();
    private Map<String, ThreatIntelligence> hashSha1Map = new HashMap<>();
    private Map<String, ThreatIntelligence> hashSha256Map = new HashMap<>();
    private Map<String, ThreatIntelligence> emailMap = new HashMap<>();
    private String jdbcUrl;
    private String userName;
    private String password;
    private String keyword;

    public KeywordMatchProcess(String jdbcUrl, String userName, String password, String keyword) {
        this.jdbcUrl = jdbcUrl;
        this.userName = userName;
        this.password = password;
        this.keyword = keyword;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        DriverManager.registerDriver(new Driver());
        connection = DriverManager.getConnection(jdbcUrl, userName, password);//获取连接
        //获取最新日期
        String sql1 = "SELECT DISTINCT(time) AS last_date FROM threat_indicator ORDER BY time DESC limit 1;";//查询最新时间
        ps1 = connection.prepareStatement(sql1);
        ResultSet rs = ps1.executeQuery();

        String lastDate = "";
        while (rs.next()) {
            lastDate = rs.getString("last_date").trim();
        }
        if ("".equals(lastDate)) {
            String currentDate = TimeUtils.getCurrentDate("yyyyMMdd");
            lastDate = TimeUtils.getPreDate(currentDate, 1);
        }


        LOG.info("========== Last date is: " + lastDate + "===========");
        System.out.println("========== Last date is: " + lastDate + "===========");
        String sql2 = "select labels from threat_indicator where time='" + lastDate + "'" ;//todo  从威胁情报库里读取数据
        ps2 = connection.prepareStatement(sql2);
        ResultSet rs2 = ps2.executeQuery();
        while (rs2.next()) {
            String labels = rs2.getString("labels");
            JSONArray jsonArray = JSON.parseArray(labels);
            for (int i = 0; i < jsonArray.size(); i++) {
                JSONObject threatIntelligenceObj = jsonArray.getJSONObject(i);
                String geo = threatIntelligenceObj.getString("geo");
                String type = threatIntelligenceObj.getString("type");
                String value = threatIntelligenceObj.getString("value");
                String reputation = threatIntelligenceObj.getString("reputation");
                //ipv4
                if (ThreatIntelligenceType.FEED_IPV4.value.equals(type)) {
                    ip4Map.put(value, new ThreatIntelligence(geo, reputation, type, value));
                }
                if (ThreatIntelligenceType.FEED_DOMAIN.value.equals(type)) {
                    domainMap.put(value, new ThreatIntelligence(geo, reputation, type, value));
                }
                if (ThreatIntelligenceType.FEED_URL.value.equals(type)) {
                    urlMap.put(value, new ThreatIntelligence(geo, reputation, type, value));
                }
                if (ThreatIntelligenceType.FEED_HASH_SHA1.value.equals(type)) {
                    hashSha1Map.put(value, new ThreatIntelligence(geo, reputation, type, value));
                }
                if (ThreatIntelligenceType.FEED_HASH_SHA256.value.equals(type)) {
                    hashSha256Map.put(value, new ThreatIntelligence(geo, reputation, type, value));
                }
                if (ThreatIntelligenceType.FEED_EMAIL.value.equals(type)) {
                    emailMap.put(value, new ThreatIntelligence(geo, reputation, type, value));
                }

            }
        }
    }


    @Override
    public void processElement(Dpi dpi, Context ctx, Collector<AlarmResult> alarmResultCollector) throws Exception {
        //1、关键字匹配
        List<String> keywords = Arrays.asList(keyword.split(","));
        String fileName = dpi.getFileName();
        for (String kw : keywords) {
            if (fileName.contains(kw)) {
                AlarmResult alarmResult = new AlarmResult(dpi.getSrcIPAddress(), dpi.getDestIPAddress(), "", "", dpi.getTrafficSize(), dpi.getFileName(),
                        keyword, "", "", "", AlarmType.SUSPICIOUS.value, dpi.getId());
                alarmResultCollector.collect(alarmResult);
                break;
            }
        }
        //2、威胁情报库匹配
        String srcIPAddress = dpi.getSrcIPAddress();
        String destIPAddress = dpi.getDestIPAddress();
        String url = dpi.getUrl();
        String domainName = dpi.getDomainName();
        processIntelligence(dpi, alarmResultCollector, ip4Map, srcIPAddress);
        processIntelligence(dpi, alarmResultCollector, ip4Map, destIPAddress);
        processIntelligence(dpi, alarmResultCollector, urlMap, url);
        processIntelligence(dpi, alarmResultCollector, domainMap, domainName);
    }

    /**
     * 威胁情报关联分析
     *
     * @param dpi
     * @param out
     * @param intelligenceMap
     * @param sourceKey
     */
    public void processIntelligence(Dpi dpi, Collector<AlarmResult> out, Map<String, ThreatIntelligence> intelligenceMap, String sourceKey) {
        String traceIds = dpi.getId();
        ThreatIntelligence srcipaddressThreatIntelligence = intelligenceMap.getOrDefault(sourceKey, null);
        if (srcipaddressThreatIntelligence != null) {
            String geo = srcipaddressThreatIntelligence.getGeo();
            String reputation = srcipaddressThreatIntelligence.getReputation();
            JSONArray reputations = JSON.parseArray(reputation);

            for (int i = 0; i < reputations.size(); i++) {
                JSONObject reputationsJSONObject = reputations.getJSONObject(i);
                String score = reputationsJSONObject.getString("score");
                String category = reputationsJSONObject.getString("category");
                AlarmResult alarmResult = new AlarmResult(dpi.getSrcIPAddress(), dpi.getDestIPAddress(), "", "", dpi.getTrafficSize(), dpi.getFileName(),
                        keyword, geo, category, score, AlarmType.SUSPICIOUS.value, traceIds);
                out.collect(alarmResult);
            }
        }
    }
}
