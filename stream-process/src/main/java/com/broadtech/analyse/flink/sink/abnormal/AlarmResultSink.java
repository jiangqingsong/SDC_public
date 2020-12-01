package com.broadtech.analyse.flink.sink.abnormal;

import com.alibaba.fastjson.JSON;
import com.broadtech.analyse.constants.ss.EvenAlarmConstant;
import com.broadtech.analyse.pojo.abnormal.AlarmResult;
import com.broadtech.analyse.pojo.main.AlarmEvenUnify1;
import org.apache.commons.dbcp.BasicDataSource;
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
 * @description 告警信息入库
 * @date 2020-08-22 11:18
 */
public class AlarmResultSink extends RichSinkFunction<List<AlarmResult>> {
    private static Logger LOG = Logger.getLogger(AlarmResultSink.class);
    private PreparedStatement ps;
    private BasicDataSource dataSource;
    private Connection connection;
    private String jdbcUrl;
    private String userName;
    private String password;
    private KafkaProducer<String, String> producer;
    private String broker;
    private String topic;

    public AlarmResultSink(String jdbcUrl, String userName, String password, String broker, String topic) {
        this.jdbcUrl = jdbcUrl;
        this.userName = userName;
        this.password = password;
        this.broker = broker;
        this.topic = topic;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        dataSource = new BasicDataSource();
        connection = getCon(dataSource);
        String sql = "insert into  " + EvenAlarmConstant.TAB_NAME_EVEN_ALARM + "(src_ip_address, dest_ip_address, window_start_time, window_end_time," +
                "traffic_size, file_name, keyword, threat_geo, threat_type, threat_score, second_event_type, trace_log_ids)" +
                "values (?,?,?,?,?,?,?,?,?,?,?,?);";
        ps = this.connection.prepareStatement(sql);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", broker);
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(properties);
    }

    @Override
    public void invoke(List<AlarmResult> value, Context context) throws Exception {
        for(AlarmResult alarmResult: value){
            addBatch(alarmResult);
        }
        int[] count = ps.executeBatch();//批量后执行
        System.out.println(EvenAlarmConstant.TAB_NAME_EVEN_ALARM + "(dpi) 成功了插入了" + count.length + "行数据");
        LOG.info(EvenAlarmConstant.TAB_NAME_EVEN_ALARM + "(dpi)  成功了插入了" + count.length + "行数据");
    }
    public void addBatch(AlarmResult alarmResult) throws SQLException {
        //alarm_type  告警类型 1-C&C异常外联 2-可疑数据外传 3-扫描探测 4-内网失陷主机检测

        String alarmType = "";
        switch (alarmResult.getAlarmType()){
            case 1:  alarmType = "C&C异常外联";
            case 2:  alarmType = "可疑数据外传";
            case 3:  alarmType = "扫描探测";
            case 4:  alarmType = "内网失陷主机检测";
        }

        ps.setString(1, alarmResult.getSrcIpAddress());
        ps.setString(2, alarmResult.getDestIpAddress());
        ps.setString(3, alarmResult.getWindowStart());
        ps.setString(4, alarmResult.getWindowEnd());
        ps.setString(5, alarmResult.getTrafficSize());
        ps.setString(6, alarmResult.getFileName());
        ps.setString(7, alarmResult.getKeyword());
        ps.setString(8, alarmResult.getGeo());
        ps.setString(9, alarmResult.getCategory());
        ps.setString(10, alarmResult.getScore());
        ps.setString(11, alarmType);
        ps.setString(12, alarmResult.getTraceIds());
        ps.addBatch();
        AlarmEvenUnify1 alarm = new AlarmEvenUnify1();
        alarm.setEvent_name(alarmType);
        alarm.setEvent_desc("");
        alarm.setFirst_event_type("");
        alarm.setSecond_event_type(alarmType);
        alarm.setThird_event_type("");
        alarm.setEvent_grade("");
        alarm.setDiscovery_time("");
        alarm.setAlarm_times("");
        alarm.setDevice_ip_address("");
        alarm.setDevice_type("");
        alarm.setDevice_factory("");
        alarm.setDevice_model("");
        alarm.setDevice_name("");
        alarm.setProtocol("");
        alarm.setMethod("");
        alarm.setUrl("");
        alarm.setNet_interface("");
        alarm.setVlan_id("");
        alarm.setSrc_ip_address(alarmResult.getSrcIpAddress());
        alarm.setSrc_port("");
        alarm.setSrc_ip_local("");
        alarm.setSrc_mac_address("");
        alarm.setDest_ip_address(alarmResult.getDestIpAddress());
        alarm.setDest_port("");
        alarm.setDest_mac_address("");
        alarm.setAffect_device("");
        alarm.setAttacker_source_type("");
        alarm.setAttacker_source("");
        alarm.setAttack_technique("");
        alarm.setAction("");
        alarm.setRule_id("");
        alarm.setDanger_level("");
        alarm.setDanger_explain("");
        alarm.setThreat_type(alarmResult.getCategory());
        alarm.setThreat_geo(alarmResult.getGeo());
        alarm.setThreat_score(alarmResult.getScore());
        alarm.setWindow_start_time(alarmResult.getWindowStart());
        alarm.setWindow_end_time(alarmResult.getWindowEnd());
        alarm.setTraffic_size("");
        alarm.setFile_name("");
        alarm.setKeyword("");
        alarm.setTrace_log_ids(alarmResult.getTraceIds());
        //发送kafka
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, JSON.toJSONString(alarm));
        producer.send(record);
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
