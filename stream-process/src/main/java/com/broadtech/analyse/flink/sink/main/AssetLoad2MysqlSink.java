package com.broadtech.analyse.flink.sink.main;

import com.broadtech.analyse.constants.asset.AssetConstants;
import com.broadtech.analyse.pojo.cmcc.AssetAgentOrigin;
import com.broadtech.analyse.pojo.main.AssetLoadInfo;
import com.broadtech.analyse.pojo.main.VulnUnify;
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
public class AssetLoad2MysqlSink extends RichSinkFunction<VulnUnify> {
    private static Logger LOG = Logger.getLogger(AssetLoad2MysqlSink.class);
    private PreparedStatement ps;
    private PreparedStatement ps2;
    private BasicDataSource dataSource;
    private Connection connection;
    private String jdbcUrl;
    private String userName;
    private String password;
    private String sql2 = "insert into  " + AssetConstants.TAB_NAME_LABEL + "(device_ip_address)" +
            "values (?);";
    private String sql = "insert into " + AssetConstants.TAB_NAME_LVULNERABILITY + "(ip, cnvd_number, lm_number, source, insert_time)values(?,?,?,?,?);";

    public AssetLoad2MysqlSink(String jdbcUrl, String userName, String password) {
        this.jdbcUrl = jdbcUrl;
        this.userName = userName;
        this.password = password;
    }

    /**
     * @param parameters 连接信息准备
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
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

    public void preStatement2(String ip) throws SQLException {
        ps2.setString(1, ip);
    }

    /**
     * 漏洞库中间表
     *
     * @param vulnUnify
     * @throws SQLException
     */
    public void preStatement(VulnUnify vulnUnify) throws SQLException {

        ps.setString(1, vulnUnify.getIp());
        ps.setString(2, vulnUnify.getCnvd_number());
        ps.setString(3, "");//没有绿盟的漏洞信息
        ps.setInt(4, 1);//1代表漏洞来源CNVD
        ps.setString(5, TimeUtils.convertTimestamp2Date(System.currentTimeMillis(), "yyyy-MM-dd hh:mm:ss"));//1代表漏洞来源CNVD
        ps.addBatch();

    }

    /**
     * 每条数据的插入都要调用一次 invoke() 方法
     *
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(VulnUnify vulnUnify, Context context) throws Exception {
        dataSource = new BasicDataSource();
        connection = getCon(dataSource);
        ps = this.connection.prepareStatement(sql);
        ps2 = this.connection.prepareStatement(sql2);
        //遍历数据集合
        preStatement(vulnUnify);
        preStatement2(vulnUnify.getIp());
        try {
            ps.execute();
            ps2.execute();
            LOG.info(AssetConstants.TAB_NAME_LVULNERABILITY + "(agent): 成功了插入了1行数据");
            LOG.info( AssetConstants.TAB_NAME_LABEL + "(agent): 成功了插入了1行数据");

        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        } finally {
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
