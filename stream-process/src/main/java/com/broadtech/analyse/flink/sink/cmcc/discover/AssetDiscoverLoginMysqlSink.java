package com.broadtech.analyse.flink.sink.cmcc.discover;

import com.alibaba.fastjson.JSON;
import com.broadtech.analyse.constants.asset.AgentCollectConstant;
import com.broadtech.analyse.constants.asset.AssetFindConstant;
import com.broadtech.analyse.pojo.cmcc.AssetAgentOrigin;
import com.broadtech.analyse.pojo.cmcc.find.AssetFindLogin;
import com.broadtech.analyse.task.cmcc.AssetDiscoverLogin2MysqlV2;
import com.broadtech.analyse.util.KafkaUtils;
import com.mysql.cj.exceptions.CJCommunicationsException;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import java.net.SocketException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * @author leo.J
 * @description Agent采集资产数据mysql
 * @date 2020-05-28 15:37
 */
public class AssetDiscoverLoginMysqlSink extends RichSinkFunction<List<AssetFindLogin>> {
    private static Logger LOG = Logger.getLogger(AssetDiscoverLoginMysqlSink.class);
    private PreparedStatement ps;
    private BasicDataSource dataSource;
    private Connection connection;
    private String jdbcUrl;
    private String userName;
    private String password;
    //private String broker;
    //private String topic;

    private KafkaProducer<String, String> producer;

    public AssetDiscoverLoginMysqlSink(/*String broker, String topic, */String jdbcUrl, String userName, String password) {
        this.jdbcUrl = jdbcUrl;
        this.userName = userName;
        this.password = password;
        //this.broker = broker;
        //this.topic = topic;
    }

    /**
     * @param parameters 连接信息准备
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        //kafka producer
        /*Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", broker);
        properties.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(properties);*/

        super.open(parameters);
        /*dataSource = new BasicDataSource();
        connection = getCon(dataSource);
        String sql = "insert into  " + AssetFindConstant.TAB_NAME_LOGIN + "(resource_name,task_id,scan_time,switch_ip," +
                "level,arp,neighbor,ipv4_route,ipv6_route)" +
                "values (?,?,?,?,?,?,?,?,?);";
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


    public void addBatch(AssetFindLogin findLogin) throws SQLException {
        ps.setString(1, findLogin.getResourceName());
        ps.setString(2, findLogin.getTaskID());
        ps.setString(3, findLogin.getScanTime());
        ps.setString(4, findLogin.getSwitchIP());
        ps.setString(5, findLogin.getLevel());
        ps.setString(6, JSON.toJSONString(findLogin.getArp()));
        ps.setString(7, JSON.toJSONString(findLogin.getNeighbor()));
        ps.setString(8, JSON.toJSONString(findLogin.getIpv4Route()));
        ps.setString(9, JSON.toJSONString(findLogin.getIpv6Route()));
        ps.addBatch();
    }

    /**
     * 每条数据的插入都要调用一次 invoke() 方法
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(List<AssetFindLogin> findLogins, Context context) throws Exception {
        dataSource = new BasicDataSource();
        connection = getCon(dataSource);
        String sql = "insert into  " + AssetFindConstant.TAB_NAME_LOGIN + "(resource_name,task_id,scan_time,switch_ip," +
                "level,arp,neighbor,ipv4_route,ipv6_route)" +
                "values (?,?,?,?,?,?,?,?,?);";
        ps = this.connection.prepareStatement(sql);
        Set<String> taskIds = new HashSet<>();
        //遍历数据集合
        for (AssetFindLogin findLogin : findLogins) {
            addBatch(findLogin);
            taskIds.add(findLogin.getTaskID());
        }
        /*for(String taskId: taskIds){
            //send kafka message
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, taskId + "_" + System.currentTimeMillis());
            producer.send(record);
        }*/
        try {
            int[] count = ps.executeBatch();//批量后执行
            LOG.info(AssetFindConstant.TAB_NAME_LOGIN + ": 成功了插入了" + count.length + "行数据");
        } catch (Exception e){
            LOG.error(e.getMessage());
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
        }
        catch (Exception e) {
            LOG.error("-----------mysql get connection has exception , msg = " + e.getMessage());
        }
        return con;

    }
}
