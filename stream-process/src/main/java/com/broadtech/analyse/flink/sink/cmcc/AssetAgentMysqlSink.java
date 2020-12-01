package com.broadtech.analyse.flink.sink.cmcc;

import com.broadtech.analyse.constants.asset.AgentCollectConstant;
import com.broadtech.analyse.pojo.cmcc.AssetAgentOrigin;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * @author leo.J
 * @description Agent采集资产数据mysql
 * @date 2020-05-28 15:37
 */
public class AssetAgentMysqlSink extends RichSinkFunction<List<Tuple2<AssetAgentOrigin, String>>> {
    private PreparedStatement ps;
    private BasicDataSource dataSource;
    private Connection connection;
    private String jdbcUrl;
    private String userName;
    private String password;

    public AssetAgentMysqlSink(String jdbcUrl, String userName, String password) {
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
        //connection = DbUtils.getCon(dataSource, "/jdbc.properties");
        connection = getCon(dataSource);
        String sql = "insert into  " + AgentCollectConstant.TAB_NAME_VULNERABILITY + "(resource_name,task_id,asset_id," +
                "scan_time,device_name,device_type,device_ip_address,os_info,patch_properties,kernel_version," +
                "open_service_of_port,program_info,start_up,message_oriented_middleware,data_base_info" +
                ",vulnerability_number" +
                "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);";
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


    public void addBatch(Tuple2<AssetAgentOrigin, String> tuple) throws SQLException {
        String separator = ",";
        AssetAgentOrigin agent = tuple.f0;
        String number = tuple.f1;
        ps.setString(1, agent.getResourceName());
        ps.setString(2, agent.getTaskID());
        ps.setString(3, agent.getAssetID());
        ps.setString(4, agent.getScanTime());
        ps.setString(5, agent.getDeviceName());
        ps.setString(6, agent.getDeviceType());
        ps.setString(7, StringUtils.join(agent.getDeviceIpAddresss(), separator));
        ps.setString(8, agent.getOSInfo());
        ps.setString(9, StringUtils.join(agent.getPatchProperties(), separator));
        ps.setString(10, agent.getKernelVersion());
        ps.setString(11, StringUtils.join(agent.getOpenServiceOfPorts(), separator));
        ps.setString(12, StringUtils.join(agent.getProgramInfos(), separator));
        ps.setString(13, StringUtils.join(agent.getMessageOrientedMiddlewares(), separator));
        ps.setString(14, StringUtils.join(agent.getDataBaseInfos(), separator));
        //number
        ps.setString(15, number);
        ps.addBatch();
    }

    /**
     * 每条数据的插入都要调用一次 invoke() 方法
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(List<Tuple2<AssetAgentOrigin, String>> tuples, Context context) throws Exception {
        String date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());

        //遍历数据集合
        for (Tuple2<AssetAgentOrigin, String> tuple : tuples) {
            addBatch(tuple);
        }
        int[] count = ps.executeBatch();//批量后执行
        System.out.println("成功了插入了" + count.length + "行数据");
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
