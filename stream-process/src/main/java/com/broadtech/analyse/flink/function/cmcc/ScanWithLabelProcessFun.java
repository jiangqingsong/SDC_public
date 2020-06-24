package com.broadtech.analyse.flink.function.cmcc;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.broadtech.analyse.constants.asset.ScanCollectConstant;
import com.broadtech.analyse.pojo.cmcc.*;
import com.broadtech.analyse.pojo.cmcc.find.RetKafkaMessage;
import com.broadtech.analyse.util.KafkaUtils;
import com.mysql.cj.jdbc.Driver;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;

/**
 * @author jiangqingsong
 * @description
 * @date 2020-06-11 17:15
 */
public class ScanWithLabelProcessFun extends ProcessFunction<AssetScanOrigin, Tuple2<AssetScanOrigin, Tuple3<String, String, String>>> {
    private static Logger LOG = Logger.getLogger(ScanWithLabelProcessFun.class);
    private Connection connection2 = null;
    private PreparedStatement ps2 = null;
    private volatile boolean isRunning = true;

    private String jdbcUrl;
    private String userName;
    private String password;

    private String broker;
    private String topic;

    private Map<String, Tuple3<Integer, String, String>> labelMap;
    private KafkaProducer<String, String> producer;
    private Timer timer;
    public ScanWithLabelProcessFun(String broker, String topic, String jdbcUrl, String userName, String password) {
        this.jdbcUrl = jdbcUrl;
        this.userName = userName;
        this.password = password;
        this.broker = broker;
        this.topic = topic;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        //
        //kafka producer
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", broker);
        properties.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(properties);

        labelMap = new HashMap<>();
        //开启一个定时任务，定期更新配置数据
        timer = new Timer(true);
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                try {
                    getData();
                    LOG.info("标签库更新！");
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
        connection2 = DriverManager.getConnection(jdbcUrl, userName, password);
        String sql2 = "select l.id as id,l.label_1 as label1,l.label_2 as label2,k.keyword as keyword from label l join label_key k on l.id = k.label_id";
        ps2 = connection2.prepareStatement(sql2);

        if (isRunning) {
            ResultSet rs2 = ps2.executeQuery();
            while (rs2.next()) {
                int id = rs2.getInt("id");
                String label1 = rs2.getString("label1");
                String label2 = rs2.getString("label2");
                String keyword = rs2.getString("keyword");
                labelMap.put(keyword, Tuple3.of(id, label1, label2));
            }
            LOG.info("======= labelMap size======" + labelMap.size());
        }

    }

    @Override
    public void processElement(AssetScanOrigin scan, Context context, Collector<Tuple2<AssetScanOrigin, Tuple3<String, String, String>>> out) throws Exception {
        if(labelMap != null){
            //待检测漏洞的Tuple<name, version>列表
            List<Tuple2<String, String>> toCheckList = new ArrayList<>();

            String taskID = scan.getTaskID();
            List<MessageOrientedMiddlewareScan> messageOrientedMiddlewares = scan.getMessageOrientedMiddleware();
            List<DataBaseInfoScan> dataBaseInfos = scan.getDataBaseInfos();
            List<OpenServiceOfPort> openServiceOfPorts = scan.getOpenServiceOfPort();
            String systemFingerprintInfo = scan.getSystemFingerprintInfo();
            for(MessageOrientedMiddlewareScan m: messageOrientedMiddlewares){
                toCheckList.add(Tuple2.of(m.getName(), m.getVersion()));
            }
            for(DataBaseInfoScan d: dataBaseInfos){
                toCheckList.add(Tuple2.of(d.getName(), d.getVersion()));
            }
            for(OpenServiceOfPort o: openServiceOfPorts){
                toCheckList.add(Tuple2.of(o.getName(), o.getVersion()));
            }

            String labelInputInfo = StringUtils.join(toCheckList, ",")
                    + "," + scan.getOSInfo() + "," + systemFingerprintInfo
                    + "," + scan.getDeviceType() + "," +scan.getRuning();

            Set<Tuple3<Integer, String, String>> matchedLabels = matchLabel(labelInputInfo);

            boolean isSwitchOrRoute = false;
            if(matchedLabels.size() == 0){
                out.collect(Tuple2.of(scan, Tuple3.of("", "", "")));
            }else {
                String separator = ",";
                List<String> labelIds = new ArrayList<>();
                List<String> type1List = new ArrayList<>();
                List<String> type2List = new ArrayList<>();
                for(Tuple3<Integer, String, String> label: matchedLabels){
                    labelIds.add(String.valueOf(label.f0));
                    type1List.add(label.f1);
                    type2List.add(label.f2);
                    //如果是资产发现下发的扫描任务且标签为交换机或者路由器，就发kafka
                    if(label.f0 == 2 || label.f0 == 3){
                        isSwitchOrRoute = true;
                    }
                }
                if(isSwitchOrRoute){
                    //send kafka message
                    JSONObject jsonObject = new JSONObject();
                    jsonObject.put("TaskID", taskID);
                    jsonObject.put("IPAddress", scan.getDeviceIPAddress());
                    if(ScanCollectConstant.RESOURCE_NAME_TAG.equals(scan.getResourceName())){
                        ProducerRecord<String, String> record = new ProducerRecord<>(topic, jsonObject.toJSONString());
                        producer.send(record);
                        LOG.info("标签匹配" + scan.getDeviceIPAddress() + "为交换机/路由器！");
                    }
                }
                out.collect(Tuple2.of(scan, Tuple3.of(StringUtils.join(labelIds, separator), StringUtils.join(type1List, separator), StringUtils.join(type2List, separator))));
            }
        }
    }

    /**
     *
     * @param input 指纹信息
     * @return labels
     */
    public Set<Tuple3<Integer, String, String>> matchLabel(String input){
        Set<Tuple3<Integer, String, String>> labels = new HashSet<Tuple3<Integer, String, String>>();
        Set<String> keywords = labelMap.keySet();
        for(String keyword: keywords){
            if(input.toLowerCase().contains(keyword.toLowerCase())){
                labels.add(labelMap.get(keyword));
            }
        }
        return labels;
    }
}
