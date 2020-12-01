package com.broadtech.analyse.flink.sink.cmcc;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.broadtech.analyse.constants.asset.AssetConstants;
import com.broadtech.analyse.flink.sink.cmcc.discover.AssetDiscoverScanMysqlSink;
import com.broadtech.analyse.pojo.cmcc.AssetAgentOrigin;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;

/**
 * @author leo.J
 * @description Agent采集资产数据mysql
 * @date 2020-05-28 15:37
 */
public class AssetAgentWithLabelMysqlSink extends RichSinkFunction<List<Tuple2<AssetAgentOrigin, Tuple3<String, String, String>>>> {
    private static Logger LOG = Logger.getLogger(AssetAgentWithLabelMysqlSink.class);
    private PreparedStatement ps;
    private BasicDataSource dataSource;
    private Connection connection;
    private String jdbcUrl;
    private String userName;
    private String password;
    private String sinkTable;
    private Integer collectType;


    public AssetAgentWithLabelMysqlSink(String sinkTable, String jdbcUrl, String userName, String password) {
        this.jdbcUrl = jdbcUrl;
        this.userName = userName;
        this.password = password;
        this.collectType = AssetConstants.AGENT.equals(sinkTable) ? 2 : 3;
        this.sinkTable = sinkTable;
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


    public void addBatch(Tuple2<AssetAgentOrigin, Tuple3<String, String, String>> tuple) throws SQLException {
        AssetAgentOrigin agent = tuple.f0;
        List<String> deviceIpAddresss = agent.getDeviceIpAddresss();
        for (String deviceIpAddress : deviceIpAddresss) {
            Tuple3<String, String, String> labelInfo = tuple.f1;
            ps.setString(1, agent.getResourceName());
            ps.setString(2, agent.getTaskID());
            ps.setString(3, agent.getAssetID());
            ps.setString(4, agent.getScanTime());
            ps.setString(5, agent.getDeviceName());
            ps.setString(6, agent.getDeviceType());
            ps.setString(7, deviceIpAddress);//String
            ps.setString(8, agent.getOSInfo());
            ps.setString(9, JSON.toJSONString(agent.getPatchProperties()));//String
            ps.setString(10, agent.getKernelVersion());
            ps.setString(11, JSON.toJSONString(agent.getOpenServiceOfPorts()));//Integer
            ps.setString(12, JSON.toJSONString(agent.getProgramInfos()));
            ps.setString(13, JSON.toJSONString(agent.getMessageOrientedMiddlewares()));
            ps.setString(14, JSON.toJSONString(agent.getDataBaseInfos()));
            //label
            ps.setString(15, labelInfo.f0);
            ps.setString(16, labelInfo.f1);
            ps.setString(17, labelInfo.f2);
            ps.setInt(18, collectType);
            ps.addBatch();
        }
    }

    /**
     * 每条数据的插入都要调用一次 invoke() 方法
     *
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(List<Tuple2<AssetAgentOrigin, Tuple3<String, String, String>>> tuples, Context context) throws Exception {
        dataSource = new BasicDataSource();
        connection = getCon(dataSource);
        String sql = "insert into  " + AssetConstants.TAB_NAME_LABEL + "(resource_name,task_id,asset_id," +
                "scan_time,device_name,device_type,device_ip_address,os_info,patch_properties,kernel_version," +
                "open_service_of_port,program_info,message_oriented_middleware,data_base_info" +
                ",label_id,label_type1,label_type2,collect_type)" +
                "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);";
        ps = this.connection.prepareStatement(sql);
        //遍历数据集合
        for (Tuple2<AssetAgentOrigin, Tuple3<String, String, String>> tuple : tuples) {
            addBatch(tuple);
        }
        try {
            int[] count = ps.executeBatch();//批量后执行
            LOG.info(AssetConstants.TAB_NAME_LABEL + "_" + collectType + ": 成功了插入了" + count.length + "行数据");
            System.out.println(AssetConstants.TAB_NAME_LABEL + "_" + collectType + ": 成功了插入了" + count.length + "行数据");
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
