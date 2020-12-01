package com.broadtech.analyse.flink.sink.cmcc;

import com.alibaba.fastjson.JSON;
import com.broadtech.analyse.constants.asset.AssetFindConstant;
import com.broadtech.analyse.pojo.cmcc.AssetAgentOrigin;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

/**
 * @author leo.J
 * @description 资产发现数据入库
 * @date 2020-05-28 15:37
 */
public class AssetFindMysqlSink extends RichSinkFunction<List<Tuple2<AssetAgentOrigin, Tuple3<Integer, String, String>>>> {
    private PreparedStatement ps;
    private BasicDataSource dataSource;
    private Connection connection;
    private String jdbcUrl;
    private String userName;
    private String password;

    public AssetFindMysqlSink(String jdbcUrl, String userName, String password) {
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
        dataSource = new BasicDataSource();
        connection = getCon(dataSource);
        String sql = "insert into  " + AssetFindConstant.TAB_NAME_LOGIN + "(resource_name,task_id," +
                "scan_time,switch_ip,level,arp,neighbor,route)" +
                "values (?,?,?,?,?,?,?,?);";
        ps = this.connection.prepareStatement(sql);
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


    public void addBatch(Tuple2<AssetAgentOrigin, Tuple3<Integer, String, String>> tuple) throws SQLException {
        AssetAgentOrigin agent = tuple.f0;
        Tuple3<Integer, String, String> labelInfo = tuple.f1;
        ps.setString(1, agent.getResourceName());
        ps.setString(2, agent.getTaskID());
        ps.setString(3, agent.getAssetID());
        ps.setString(4, agent.getScanTime());
        ps.setString(5, agent.getDeviceName());
        ps.setString(6, agent.getDeviceType());
        ps.setString(7, JSON.toJSONString(agent.getDeviceIpAddresss()));//String
        ps.setString(8, agent.getOSInfo());
        ps.setString(9, JSON.toJSONString(agent.getPatchProperties()));//String
        ps.setString(10, agent.getKernelVersion());
        ps.setString(11, JSON.toJSONString(agent.getOpenServiceOfPorts()));//Integer
        ps.setString(12, JSON.toJSONString(agent.getProgramInfos()));
        ps.setString(13, JSON.toJSONString(agent.getMessageOrientedMiddlewares()));
        ps.setString(14, JSON.toJSONString(agent.getDataBaseInfos()));
        //label
        ps.setInt(15, labelInfo.f0);
        ps.setString(16, labelInfo.f1);
        ps.setString(17, labelInfo.f2);
        ps.addBatch();
    }

    /**
     * 每条数据的插入都要调用一次 invoke() 方法
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(List<Tuple2<AssetAgentOrigin, Tuple3<Integer, String, String>>> tuples, Context context) throws Exception {
        //String date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());

        //遍历数据集合
        for (Tuple2<AssetAgentOrigin, Tuple3<Integer, String, String>> tuple : tuples) {
            addBatch(tuple);
        }
        try {
            int[] count = ps.executeBatch();//批量后执行
            System.out.println("Label site: 成功了插入了" + count.length + "行数据");
        }catch (Exception e){
            e.printStackTrace();
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
            System.out.println("创建连接池：" + con);
        } catch (Exception e) {
            System.out.println("-----------mysql get connection has exception , msg = " + e.getMessage());
            System.out.println("url: "+ jdbcUrl + ", username: " + userName + ", password: " + password);
        }
        return con;

    }
}
