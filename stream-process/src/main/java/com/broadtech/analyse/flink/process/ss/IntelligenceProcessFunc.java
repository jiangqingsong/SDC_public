package com.broadtech.analyse.flink.process.ss;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.broadtech.analyse.pojo.cmcc.AssetAgentOrigin;
import com.broadtech.analyse.pojo.cmcc.Vulnerability;
import com.broadtech.analyse.pojo.ss.SecurityLog;
import com.broadtech.analyse.pojo.ss.ThreatIntelligence;
import com.broadtech.analyse.pojo.ss.ThreatIntelligence2;
import com.broadtech.analyse.pojo.ss.ThreatIntelligenceMaps;
import com.broadtech.analyse.util.VulnerabilityUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import com.mysql.cj.jdbc.Driver;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;

/**
 * @author leo.J
 * @description 情报库、漏洞库关联分析
 * @date 2020-08-10 15:28
 */
public class IntelligenceProcessFunc extends BroadcastProcessFunction<Tuple3<SecurityLog, Integer, Long>, ThreatIntelligenceMaps, Tuple5<SecurityLog, Integer, Long, String, ThreatIntelligence2>> {
    private static Logger LOG = Logger.getLogger(IntelligenceProcessFunc.class);
    private Map<String, ThreatIntelligence> ip4Map = new HashMap<>();
    private Map<String, ThreatIntelligence> domainMap = new HashMap<>();
    private Map<String, ThreatIntelligence> emailMap = new HashMap<>();
    private Map<String, ThreatIntelligence> hashMd5Map = new HashMap<>();
    private Map<String, ThreatIntelligence> hashSha1Map = new HashMap<>();
    private Map<String, ThreatIntelligence> hashSha256Map = new HashMap<>();
    private Map<String, ThreatIntelligence> urlMap = new HashMap<>();
    final MapStateDescriptor<Void, ThreatIntelligenceMaps> configDescriptor = new MapStateDescriptor(
            "threatIntelligenceMap", Types.VOID,
            Types.POJO(ThreatIntelligenceMaps.class));

    //漏洞库相关
    private static final OutputTag<Tuple2<AssetAgentOrigin, String>> vulnerabilityTag
            = new OutputTag<Tuple2<AssetAgentOrigin, String>>("VulnerabilityTag") {};
    private Connection connection = null;
    private PreparedStatement ps1 = null;
    private volatile boolean isRunning = true;

    private String jdbcUrl;
    private String userName;
    private String password;
    private Timer timer;

    private Map<String, Vulnerability> vulnerabilityMap;
    public IntelligenceProcessFunc(String jdbcUrl, String userName, String password) {
        this.jdbcUrl = jdbcUrl;
        this.userName = userName;
        this.password = password;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        vulnerabilityMap = new HashMap<>();
        //开启一个定时任务，定期更新配置数据
        timer = new Timer(true);
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                try {
                    getData();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }, 1000, 1*30*60*1000);
    }
    @Override
    public void close() throws Exception {
        timer.cancel();
        timer = null;
    }
    /**
     * 获取漏洞数据和label数据
     * @throws Exception
     */
    public void getData() throws Exception{
        DriverManager.registerDriver(new Driver());
        connection = DriverManager.getConnection(jdbcUrl, userName, password);//获取连接
        String sql1 = "select * from vulnerability";
        ps1 = connection.prepareStatement(sql1);

        if (isRunning) {
            ResultSet rs1 = ps1.executeQuery();
            while (rs1.next()) {
                String description = rs1.getString("description");
                String discoverername = rs1.getString("discoverername");
                String formalway = rs1.getString("formalway");
                String isevent = rs1.getString("isevent");
                String number = rs1.getString("number");
                String opentime = rs1.getString("opentime");
                String patchdescription = rs1.getString("patchdescription");
                String patchname = rs1.getString("patchname");
                String product = rs1.getString("product");
                String serverity = rs1.getString("serverity");
                String submittime = rs1.getString("submittime");
                String title = rs1.getString("title");
                if("".equals(product)){
                    continue;
                }
                Vulnerability vulnerability = new Vulnerability();
                vulnerability.setDescription(description);
                vulnerability.setDiscoverername(discoverername);
                vulnerability.setFormalway(formalway);
                vulnerability.setIsevent(isevent);
                vulnerability.setNumber(number);
                vulnerability.setOpentime(opentime);
                vulnerability.setPatchdescription(patchdescription);
                vulnerability.setPatchname(patchname);
                vulnerability.setServerity(serverity);
                vulnerability.setSubmittime(submittime);
                vulnerability.setTitle(title);
                vulnerability.setProduct(product);
                vulnerabilityMap.put(product, vulnerability);
            }
            LOG.info("====== valnerabilityMap size ======" + vulnerabilityMap.size());
        }

    }

    /**
     * 威胁情报碰撞目前支持：
     * 1、url
     * 2、ip
     * 3、email
     * @param value
     * @param ctx
     * @param out
     * @throws Exception
     */
    @Override
    public void processElement(Tuple3<SecurityLog, Integer, Long> value, ReadOnlyContext ctx, Collector<Tuple5<SecurityLog, Integer, Long, String, ThreatIntelligence2>> out) throws Exception {

        ReadOnlyBroadcastState<Void, ThreatIntelligenceMaps> broadcastState = ctx.getBroadcastState(configDescriptor);
        ThreatIntelligenceMaps threatIntelligenceMaps = broadcastState.get(null);
        /*while (threatIntelligenceMaps == null){
            Thread.sleep(5000L);
        }*/
        if(threatIntelligenceMaps != null){
            System.out.println("broadcast is not null.");
        }

        SecurityLog securityLog = value.f0;
        Integer count = value.f1;
        Long windowEnd = value.f2;
        //漏洞库关联 todo
        String softwarename = securityLog.getSoftwarename();
        String softwareversion = securityLog.getSoftwareversion();

        Set<String> products = vulnerabilityMap.keySet();
        Set<String> matchedVulnerabilitySet = new HashSet<>();
        for(String product: products){
            if(product.contains(softwarename)){
                matchedVulnerabilitySet.add(vulnerabilityMap.get(product).getNumber());
                break;//随机匹配一条漏洞
            }
        }
        String number =  StringUtils.join(matchedVulnerabilitySet, ",");

        //ipv4
        String srcipaddress = securityLog.getSrcipaddress();
        String destipaddress = securityLog.getDestipaddress();
        String deviceipaddress = securityLog.getDeviceipaddress();
        processIntelligence(securityLog, count, windowEnd, number,  out, ip4Map, srcipaddress);
        processIntelligence(securityLog, count, windowEnd, number, out, ip4Map, destipaddress);
        processIntelligence(securityLog, count, windowEnd, number, out, ip4Map, deviceipaddress);

        //domain
        //processIntelligence(securityLog, count, out, domainMap, xxx);

        //url
        processIntelligence(securityLog, count, windowEnd, number, out, urlMap, securityLog.getUrl());

        //email
        processIntelligence(securityLog, count, windowEnd, number, out, emailMap, securityLog.getMail());

        //md5
        //processIntelligence(securityLog, count, out, hashMd5Map, xxx);

        //sha1
        //processIntelligence(securityLog, count, out, hashSha1Map, xxx);

        //sha256
        //processIntelligence(securityLog, count, out, hashSha256Map, xxxx);
    }

    /**
     * 威胁情报关联分析
     * @param securityLog 原始日志
     * @param count 原始事件数
     * @param out
     * @param intelligenceMap 威胁情报Map
     * @param sourceKey 待关联key字段
     */
    public void processIntelligence(SecurityLog securityLog, Integer count, Long windowEnd, String number, Collector<Tuple5<SecurityLog, Integer, Long, String, ThreatIntelligence2>> out, Map<String, ThreatIntelligence> intelligenceMap, String sourceKey){
        ThreatIntelligence srcipaddressThreatIntelligence = intelligenceMap.getOrDefault(sourceKey, null);
        if(srcipaddressThreatIntelligence != null){
            String geo = srcipaddressThreatIntelligence.getGeo();
            String type = srcipaddressThreatIntelligence.getType();
            String value1 = srcipaddressThreatIntelligence.getValue();

            String reputation = srcipaddressThreatIntelligence.getReputation();
            JSONArray reputations = JSON.parseArray(reputation);

            for(int i=0;i<reputations.size();i++){
                JSONObject reputationsJSONObject = reputations.getJSONObject(i);
                String score = reputationsJSONObject.getString("score");
                String category = reputationsJSONObject.getString("category");
                String timestamp = reputationsJSONObject.getString("timestamp");
                out.collect(Tuple5.of(securityLog, count, windowEnd, number,  new ThreatIntelligence2(geo, type, value1, score, category, timestamp)));
            }
        }
    }


    @Override
    public void processBroadcastElement(ThreatIntelligenceMaps value, Context ctx, Collector<Tuple5<SecurityLog, Integer, Long, String, ThreatIntelligence2>> out) throws Exception {
        BroadcastState<Void, ThreatIntelligenceMaps> broadcastState = ctx.getBroadcastState(this.configDescriptor);
        broadcastState.put(null, value);
        this.ip4Map = value.getIp4Map();
        this.domainMap = value.getDomainMap();
        this.urlMap = value.getUrlMap();
        this.hashMd5Map = value.getHashMd5Map();
        this.hashSha1Map = value.getHashSha1Map();
        this.hashSha256Map = value.getHashSha256Map();
        System.out.println("-------------- Load Threat Data Success！----------------");
        LOG.info("-------------- Load Threat Data Success！----------------");
    }
}
