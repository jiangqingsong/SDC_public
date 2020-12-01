package com.broadtech.analyse.flink.sink.ss;

import com.alibaba.fastjson.JSON;
import com.broadtech.analyse.constants.ss.EvenAlarmConstant;
import com.broadtech.analyse.pojo.cmcc.AssetScanOrigin;
import com.broadtech.analyse.pojo.main.AlarmEvenUnify1;
import com.broadtech.analyse.pojo.ss.SecurityLog;
import com.broadtech.analyse.pojo.ss.ThreatIntelligence2;
import com.broadtech.analyse.util.TimeUtils;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

/**
 * @author leo.J
 * @description 原始告警事件日志入库
 * 1、原始告警数据不需要和威胁情报碰撞
 * @date 2020-08-11 11:11
 */
public class OriginEventAlarm2MysqlSink extends RichSinkFunction<List<SecurityLog>> {
    private static Logger LOG = Logger.getLogger(OriginEventAlarm2MysqlSink.class);
    private PreparedStatement ps;
    private BasicDataSource dataSource;
    private Connection connection;
    private String jdbcUrl;
    private String userName;
    private String password;
    private KafkaProducer<String, String> producer;
    private String broker;
    private String topic;

    public OriginEventAlarm2MysqlSink(String jdbcUrl, String userName, String password, String broker, String topic) {
        this.jdbcUrl = jdbcUrl;
        this.userName = userName;
        this.password = password;
        this.broker = broker;
        this.topic = topic;
    }

    /**
     * @param parameters 连接信息准备
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        dataSource = new BasicDataSource();
        connection = getCon(dataSource);
        String sql = "insert into  " + EvenAlarmConstant.TAB_NAME_EVEN_ALARM + "(event_name,first_event_type,second_event_type," +
                "third_event_type,event_grade,discovery_time,alarm_times,device_ip_address,device_type,device_factory,device_model," +
                "device_name,src_ip_address,src_port,src_mac_address,dest_ip_address,dest_port,affect_device,event_desc,attacker_source_type," +
                "attacker_source,threat_type,threat_geo,threat_score,window_end_time,trace_log_ids)" +
                "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);";
        ps = this.connection.prepareStatement(sql);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", broker);
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(properties);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null) {
            connection.close();
        }
        if (ps != null) {
            ps.close();
        }
    }


    public void addBatch(Tuple2<AssetScanOrigin, String> tuple) throws SQLException {
        AssetScanOrigin scan = tuple.f0;
        String number = tuple.f1;
        ps.setString(1, scan.getResourceName());
        ps.addBatch();
    }

    /**
     * 每条数据的插入都要调用一次 invoke() 方法
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(List<SecurityLog> securityLogs, Context context) throws Exception {
        //遍历数据集合
        for(SecurityLog securitylog: securityLogs){
            addBatch(securitylog);
        }
        int[] count = ps.executeBatch();//批量后执行
        //System.out.println("成功了插入了" + count.length + "行数据");
        LOG.info("成功了插入了" + count.length + "行数据");
    }
    public void addBatch(SecurityLog securityLog) throws SQLException {
        ps.setString(1, securityLog.getEventname());
        ps.setString(2, securityLog.getFirsteventtype());
        ps.setString(3, securityLog.getSecondeventtype());
        ps.setString(4, securityLog.getThirdeventtype());
        ps.setString(5, securityLog.getEventgrade());
        ps.setString(6, securityLog.getEventgeneratetime());
        ps.setString(7, "1");//原始告警数据记为1
        ps.setString(8, securityLog.getDeviceipaddress());
        ps.setString(9, securityLog.getDevicetype());
        ps.setString(10, securityLog.getDevicefactory());
        ps.setString(11, securityLog.getDevicemodel());
        ps.setString(12, securityLog.getDevicename());
        ps.setString(13, securityLog.getSrcipaddress());
        ps.setString(14, securityLog.getSrcport());
        ps.setString(15, securityLog.getSrcmacaddress());
        ps.setString(16, securityLog.getDestipaddress());
        ps.setString(17, securityLog.getDestport());
        ps.setString(18, "");
        ps.setString(19, securityLog.getEventdesc());

        ps.setString(20, "");
        ps.setString(21, "");
        ps.setString(22, "");
        ps.setString(23, "");
        ps.setString(24, "");
        ps.setString(25, "");
        ps.setString(26, securityLog.getUuid());//每条日志代表一个事件，所以溯源就是它本身
        ps.addBatch();
        //send kafka
        String alarmTimes = String.valueOf(1);
        AlarmEvenUnify1 alarm = new AlarmEvenUnify1();
        alarm.setEvent_name(securityLog.getEventname());
        alarm.setEvent_desc(securityLog.getEventdesc());
        alarm.setFirst_event_type(securityLog.getFirsteventtype());
        alarm.setSecond_event_type(securityLog.getSecondeventtype());
        alarm.setThird_event_type(securityLog.getThirdeventtype());
        alarm.setEvent_grade(securityLog.getEventgrade());
        alarm.setDiscovery_time(securityLog.getEventgeneratetime());
        alarm.setAlarm_times(alarmTimes);
        alarm.setDevice_ip_address(securityLog.getDeviceipaddress());
        alarm.setDevice_type(securityLog.getDevicetype());
        alarm.setDevice_factory(securityLog.getDevicefactory());
        alarm.setDevice_model(securityLog.getDevicemodel());
        alarm.setDevice_name(securityLog.getDevicename());
        alarm.setProtocol("");
        alarm.setMethod("");
        alarm.setUrl("");
        alarm.setNet_interface("");
        alarm.setVlan_id("");
        alarm.setSrc_ip_address(securityLog.getSrcipaddress());
        alarm.setSrc_port(securityLog.getSrcport());
        alarm.setSrc_ip_local("");
        alarm.setSrc_mac_address("");
        alarm.setDest_ip_address(securityLog.getDestipaddress());
        alarm.setDest_port(securityLog.getDestport());
        alarm.setDest_mac_address("");
        alarm.setAffect_device("");
        alarm.setAttacker_source_type("");
        alarm.setAttacker_source("");
        alarm.setAttack_technique("");
        alarm.setAction("");
        alarm.setRule_id("");
        alarm.setDanger_level("");
        alarm.setDanger_explain("");
        alarm.setThreat_type("");
        alarm.setThreat_geo("");
        alarm.setThreat_score("");
        alarm.setWindow_start_time("");
        alarm.setWindow_end_time("");
        alarm.setTraffic_size("");
        alarm.setFile_name("");
        alarm.setKeyword("");
        alarm.setTrace_log_ids(securityLog.getUuid());
        //发送kafka
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, JSON.toJSONString(alarm));
        try {
            producer.send(record);
        }catch (Exception e){
            LOG.error("alarm info send fail." + e.getMessage());
        }
    }

    public Connection getCon(BasicDataSource dataSource) throws Exception {
        dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
        dataSource.setUrl(jdbcUrl);
        dataSource.setUsername(userName);
        dataSource.setPassword(password);
        //设置连接池的一些参数
        dataSource.setInitialSize(5);
        dataSource.setMinIdle(2);

        Connection con = null;
        try {
            con = dataSource.getConnection();
        } catch (Exception e) {
            LOG.info("-----------mysql get connection has exception , msg = " + e.getMessage());
        }
        return con;

    }
}
