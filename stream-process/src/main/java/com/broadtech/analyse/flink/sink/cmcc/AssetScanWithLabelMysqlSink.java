package com.broadtech.analyse.flink.sink.cmcc;

import com.alibaba.fastjson.JSON;
import com.broadtech.analyse.constants.asset.AgentCollectConstant;
import com.broadtech.analyse.constants.asset.ScanCollectConstant;
import com.broadtech.analyse.pojo.cmcc.AssetAgentOrigin;
import com.broadtech.analyse.pojo.cmcc.AssetScanOrigin;
import com.broadtech.analyse.task.cmcc.AssetScan2MysqlV2;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

/**
 * @author jiangqingsong
 * @description Agent采集资产数据mysql
 * @date 2020-05-28 15:37
 */
public class AssetScanWithLabelMysqlSink extends RichSinkFunction<List<Tuple2<AssetScanOrigin, Tuple3<String, String, String>>>> {
    private static Logger LOG = Logger.getLogger(AssetScanWithLabelMysqlSink.class);
    private PreparedStatement ps;
    private BasicDataSource dataSource;
    private Connection connection;
    private String jdbcUrl;
    private String userName;
    private String password;

    public AssetScanWithLabelMysqlSink(String jdbcUrl, String userName, String password) {
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
        /*dataSource = new BasicDataSource();
        //connection = DbUtils.getCon(dataSource, "/jdbc.properties");
        connection = getCon(dataSource);
        String sql = "insert into  " + ScanCollectConstant.TAB_NAME_LABEL + "(resource_name,task_id,scan_time," +
                "device_ip_address,ip_address_ownership,device_type,os_info,system_fingerprint_info," +
                "open_service_of_port,message_oriented_middleware,data_base_info,running," +
                "label_id,label_type1,label_type2)" +
                "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);";
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


    public void addBatch(Tuple2<AssetScanOrigin, Tuple3<String, String, String>> tuple) throws SQLException {
        AssetScanOrigin scan = tuple.f0;
        Tuple3<String, String, String> labelInfo = tuple.f1;
        ps.setString(1, scan.getResourceName());
        ps.setString(2, scan.getTaskID());
        ps.setString(3, scan.getScanTime());
        ps.setString(4, scan.getDeviceIPAddress());
        ps.setString(5, scan.getIPAddressOwnership());
        ps.setString(6, scan.getDeviceType());
        ps.setString(7, scan.getOSInfo());//String
        ps.setString(8, scan.getSystemFingerprintInfo());
        ps.setString(9, JSON.toJSONString(scan.getOpenServiceOfPort()));//String
        ps.setString(10, JSON.toJSONString(scan.getMessageOrientedMiddleware()));
        ps.setString(11, JSON.toJSONString(scan.getDataBaseInfos()));//Integer
        ps.setString(12, scan.getRuning());
        //label
        ps.setString(13, labelInfo.f0);
        ps.setString(14, labelInfo.f1);
        ps.setString(15, labelInfo.f2);
        ps.addBatch();
    }

    /**
     * 每条数据的插入都要调用一次 invoke() 方法
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(List<Tuple2<AssetScanOrigin, Tuple3<String, String, String>>> tuples, Context context) throws Exception {
        dataSource = new BasicDataSource();
        connection = getCon(dataSource);
        String sql = "insert into  " + ScanCollectConstant.TAB_NAME_LABEL + "(resource_name,task_id,scan_time," +
                "device_ip_address,ip_address_ownership,device_type,os_info,system_fingerprint_info," +
                "open_service_of_port,message_oriented_middleware,data_base_info,running," +
                "label_id,label_type1,label_type2)" +
                "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);";
        ps = this.connection.prepareStatement(sql);
        //遍历数据集合
        for (Tuple2<AssetScanOrigin, Tuple3<String, String, String>> tuple : tuples) {
            addBatch(tuple);
        }
        try {
            int[] count = ps.executeBatch();//批量后执行
            LOG.info(ScanCollectConstant.TAB_NAME_LABEL + ": 成功了插入了" + count.length + "行数据");
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
        } catch (Exception e) {
            LOG.info("-----------mysql get connection has exception , msg = " + e.getMessage());
        }
        return con;

    }
}
