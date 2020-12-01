package com.broadtech.analyse.flink.sink.cmcc;

import com.broadtech.analyse.constants.asset.AssetConstants;
import com.broadtech.analyse.pojo.cmcc.AssetAgentOrigin;
import com.broadtech.analyse.util.TimeUtils;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.flink.api.java.tuple.Tuple2;
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
 * @description Agent采集资产数据mysql
 * @date 2020-05-28 15:37
 */
public class AssetAgentWithVulnerMysqlSink extends RichSinkFunction<List<Tuple2<AssetAgentOrigin, String>>> {
    private static Logger LOG = Logger.getLogger(AssetAgentWithVulnerMysqlSink.class);
    private PreparedStatement ps;
    private BasicDataSource dataSource;
    private Connection connection;
    private String jdbcUrl;
    private String userName;
    private String password;
    private String sinkTable;
    private String broker;
    private String topic;
    private KafkaProducer<String, String> producer;

    public AssetAgentWithVulnerMysqlSink(String broker, String topic, String sinkTable, String jdbcUrl, String userName, String password) {
        this.jdbcUrl = jdbcUrl;
        this.userName = userName;
        this.password = password;
        this.sinkTable = sinkTable;
        this.broker = broker;
        this.topic = topic;
    }

    /**
     * @param parameters 连接信息准备
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", broker);
        properties.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(properties);

        super.open(parameters);
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


    public void addBatch(Tuple2<AssetAgentOrigin, String> tuple) throws SQLException {
        AssetAgentOrigin agent = tuple.f0;
        String number = tuple.f1;

        List<String> deviceIpAddresss = agent.getDeviceIpAddresss();
        String[] numbers = number.split(",");
        for(String ip: deviceIpAddresss){
            for(int i=0;i<numbers.length;i++){
                ps.setString(1, ip);
                ps.setString(2, numbers[i]);
                ps.setString(3, "");//没有绿盟的漏洞信息
                ps.setInt(4, 1);//1代表漏洞来源CNVD
                ps.setString(5, TimeUtils.convertTimestamp2Date(System.currentTimeMillis(), "yyyy-MM-dd hh:mm:ss"));//1代表漏洞来源CNVD
                ps.addBatch();
            }
        }

    }

    /**
     * 每条数据的插入都要调用一次 invoke() 方法
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(List<Tuple2<AssetAgentOrigin, String>> tuples, Context context) throws Exception {
        dataSource = new BasicDataSource();
        connection = getCon(dataSource);
        /*String sql = "insert into  " + sinkTable + "(resource_name,task_id,asset_id," +
                "scan_time,device_name,device_type,device_ip_address,os_info,patch_properties,kernel_version," +
                "open_service_of_port,program_info,message_oriented_middleware,data_base_info" +
                ",vulnerability_number)" +
                "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);";*/
        String sql = "insert into " + AssetConstants.TAB_NAME_LVULNERABILITY + "(ip, cnvd_number, lm_number, source, insert_time)values(?,?,?,?,?);";
        ps = this.connection.prepareStatement(sql);
        //遍历数据集合
        for (Tuple2<AssetAgentOrigin, String> tuple : tuples) {
            addBatch(tuple);
            //send kafka message
            String scanTime = tuple.f0.getScanTime();
            long timestamp;
            try {
                timestamp = TimeUtils.getTimestamp("yyyy-MM-dd HH:mm:ss", scanTime);
            }catch (Exception e){
                timestamp = System.currentTimeMillis();
            }
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, tuple.f0.getTaskID() + "_" + timestamp);
            producer.send(record);
        }
        try {
            int[] count = ps.executeBatch();//批量后执行
            LOG.info(AssetConstants.TAB_NAME_LVULNERABILITY + "(agent): 成功了插入了" + count.length + "行数据");

        }catch (Exception e){
            LOG.error(e.getMessage(), e);
        }finally {
            close();
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
            LOG.info("(Agent/Login)创建连接池：" + con);
        } catch (Exception e) {
            LOG.error("-----------mysql get connection has exception , msg = " + e.getMessage());
        }
        return con;

    }
}
