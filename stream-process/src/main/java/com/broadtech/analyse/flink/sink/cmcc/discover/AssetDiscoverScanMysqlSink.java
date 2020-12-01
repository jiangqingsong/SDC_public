package com.broadtech.analyse.flink.sink.cmcc.discover;

import com.alibaba.fastjson.JSON;
import com.broadtech.analyse.constants.asset.AssetFindConstant;
import com.broadtech.analyse.pojo.cmcc.find.AssetFindScan;
import com.broadtech.analyse.util.KafkaUtils;
import com.broadtech.analyse.util.TimeUtils;
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
 * @description Agent采集资产数据mysql
 * @date 2020-05-28 15:37
 */
public class AssetDiscoverScanMysqlSink extends RichSinkFunction<List<AssetFindScan>> {
    private static Logger LOG = Logger.getLogger(AssetDiscoverScanMysqlSink.class);
    private PreparedStatement ps;
    private BasicDataSource dataSource;
    private Connection connection;
    private String jdbcUrl;
    private String userName;
    private String password;

    private String broker;
    private String topic;

    private KafkaProducer<String, String> producer;

    public AssetDiscoverScanMysqlSink(String broker, String topic, String jdbcUrl, String userName, String password) {
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

        //kafka producer
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", broker);
        properties.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(properties);
        //
        super.open(parameters);
        /*dataSource = new BasicDataSource();
        connection = getCon(dataSource);
        String sql = "insert into  " + AssetFindConstant.TAB_NAME_SCAN + "(resource_name,task_id,scan_time,ip_list)" +
                "values (?,?,?,?);";
        ps = this.connection.prepareStatement(sql);*/
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


    public void addBatch(AssetFindScan login) throws SQLException {
        ps.setString(1, login.getResourceName());
        ps.setString(2, login.getTaskID());
        ps.setString(3, login.getScanTime());
        ps.setString(4, JSON.toJSONString(login.getIps()));
        ps.addBatch();
    }

    /**
     * 每条数据的插入都要调用一次 invoke() 方法
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(List<AssetFindScan> logins, Context context) throws Exception {
        dataSource = new BasicDataSource();
        connection = getCon(dataSource);
        String sql = "insert into  " + AssetFindConstant.TAB_NAME_SCAN + "(resource_name,task_id,scan_time,ip_list)" +
                "values (?,?,?,?);";
        ps = this.connection.prepareStatement(sql);
        //遍历数据集合
        for (AssetFindScan scan : logins) {
            addBatch(scan);
            //send kafka message
            long timestamp;
            try {
                timestamp = TimeUtils.getTimestamp("yyyy-MM-dd HH:mm:ss", scan.getScanTime());
            }catch (Exception e){
                timestamp = System.currentTimeMillis();
            }
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, scan.getTaskID() + "_" + timestamp);
            producer.send(record);
        }
        try {
            int[] count = ps.executeBatch();//批量后执行
            LOG.info(AssetFindConstant.TAB_NAME_SCAN + ": 成功了插入了" + count.length + "行数据");
        }catch (Exception e){
            LOG.error(e.getMessage(), e);
        }finally {
            close();
        }
    }

    public Connection getCon(BasicDataSource dataSource) {
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
            LOG.error("-----------mysql get connection has exception , msg = " + e.getMessage());
        }
        return con;

    }
}
