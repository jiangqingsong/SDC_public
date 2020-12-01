package com.broadtech.analyse.flink.source.ss;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.broadtech.analyse.constants.ss.ThreatIntelligenceType;
import com.broadtech.analyse.pojo.ss.ThreatIntelligence;
import com.broadtech.analyse.pojo.ss.ThreatIntelligenceMaps;
import com.broadtech.analyse.util.TimeUtils;
import com.mysql.cj.jdbc.Driver;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

/**
 * @author leo.J
 * @description 从mysql读取威胁情报库
 * @date 2020-07-17 17:16
 */
public class IntelligenceLibReader extends RichSourceFunction<ThreatIntelligenceMaps> {
    private static final Logger LOG = LoggerFactory.getLogger(IntelligenceLibReader.class);

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

    public IntelligenceLibReader(String jdbcUrl, String userName, String password) {
        this.jdbcUrl = jdbcUrl;
        this.userName = userName;
        this.password = password;
    }

    /**
     * 打开jdbc连接
     *
     * @param parameters
     * @throws Exception
     */
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
        while (rs.next()){
            lastDate = rs.getString("last_date").trim();
        }
        if("".equals(lastDate)){
            String currentDate = TimeUtils.getCurrentDate("yyyyMMdd");
            lastDate = TimeUtils.getPreDate(currentDate, 1);
        }

        String sql2 = "select labels from threat_indicator where time='" + lastDate + "' limit 10";//todo  从威胁情报库里读取数据
        ps2 = connection.prepareStatement(sql2);
    }

    /**
     * 按天更新
     * @param ctx
     * @throws Exception
     */
    public void run(SourceContext<ThreatIntelligenceMaps> ctx) throws Exception {

        //Map<String, ThreatIntelligence> threatIntelligenceMap = new HashMap<>();
        while (isRunning) {
            ResultSet rs = ps2.executeQuery();
            while (rs.next()) {
                String labels = rs.getString("labels");
                JSONArray jsonArray = JSON.parseArray(labels);
                for(int i=0; i< jsonArray.size();i++){
                    JSONObject threatIntelligenceObj = jsonArray.getJSONObject(i);
                    String geo = threatIntelligenceObj.getString("geo");
                    String type = threatIntelligenceObj.getString("type");
                    String value = threatIntelligenceObj.getString("value");
                    String reputation = threatIntelligenceObj.getString("reputation");
                    //ipv4
                    if(ThreatIntelligenceType.FEED_IPV4.value.equals(type)){
                        ip4Map.put(value, new ThreatIntelligence(geo, reputation, type, value));
                    }
                    if(ThreatIntelligenceType.FEED_DOMAIN.value.equals(type)){
                        domainMap.put(value, new ThreatIntelligence(geo, reputation, type, value));
                    }
                    if(ThreatIntelligenceType.FEED_URL.value.equals(type)){
                        urlMap.put(value, new ThreatIntelligence(geo, reputation, type, value));
                    }
                    if(ThreatIntelligenceType.FEED_HASH_SHA1.value.equals(type)){
                        hashSha1Map.put(value, new ThreatIntelligence(geo, reputation, type, value));
                    }
                    if(ThreatIntelligenceType.FEED_HASH_SHA256.value.equals(type)){
                        hashSha256Map.put(value, new ThreatIntelligence(geo, reputation, type, value));
                    }
                    if(ThreatIntelligenceType.FEED_EMAIL.value.equals(type)){
                        emailMap.put(value, new ThreatIntelligence(geo, reputation, type, value));
                    }

                }
            }
            ctx.collect(new ThreatIntelligenceMaps(ip4Map,domainMap,urlMap,hashMd5Map,hashSha1Map,hashSha256Map,emailMap));
            //threatIntelligenceMap.clear();

            //设置多久查询更新mysql数据
            Thread.sleep(1 * 24 * 60 * 60  * 1000);
        }
    }

    public void cancel() {
        try {
            super.close();
            if (connection != null) {
                connection.close();
            }
            if (ps1 != null) {
                ps1.close();
            }
            if (ps2 != null) {
                ps2.close();
            }
        } catch (Exception e) {
            LOG.error("runException{}", e);
        }
        isRunning = false;
    }
}
