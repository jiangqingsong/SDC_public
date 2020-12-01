package com.broadtech.analyse.flink.sink;

import com.alibaba.fastjson.JSONObject;
import com.broadtech.analyse.constants.asset.DpiInfoConstant;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * @author leo.J
 * @description
 * @date 2020-05-28 15:37
 */
public class DpiMysqlSink extends RichSinkFunction<List<ObjectNode>> {
    PreparedStatement ps;
    BasicDataSource dataSource;
    private Connection connection;

    /**
     * open() 方法中建立连接，这样不用每次 invoke 的时候都要建立连接和释放连接
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        dataSource = new BasicDataSource();
        connection = getConnection(dataSource);
        String sql = "insert into dpilog (src_ip_type,dst_ip_type,src_ip,src_port,src_dwmc,dst_ip,dst_port,dst_dwmc,ul_traffic,dl_traffic,ul_packtes,dl_packets,l4proto,protocol,manufacturer,asset_type,start_time,end_time,remarks,host,uri,dev_ip,dev_port,ip_type,dev_type,dev_name,sof_tname,sof_tver,vendor,os,os_ver,net_type,last_time,src_mac,dst_mac) " +
                "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);";
        ps = this.connection.prepareStatement(sql);
    }

    @Override
    public void close() throws Exception {
        super.close();
        //关闭连接和释放资源
        if (connection != null) {
            connection.close();
        }
        if (ps != null) {
            ps.close();
        }
    }


    public void addBatch(ObjectNode objectNode) throws SQLException {
        String value = objectNode.get("value").toString();
        JSONObject dpiInfo = JSONObject.parseObject(value);
        String SRC_IP_TYPE = dpiInfo.getString(DpiInfoConstant.SRC_IP_TYPE);
        String DST_IP_TYPE = dpiInfo.getString(DpiInfoConstant.DST_IP_TYPE);
        String SRC_IP = dpiInfo.getString(DpiInfoConstant.SRC_IP);
        String SRC_PORT = dpiInfo.getString(DpiInfoConstant.SRC_PORT);
        String SRC_DWMC = dpiInfo.getString(DpiInfoConstant.SRC_DWMC);
        String DST_IP = dpiInfo.getString(DpiInfoConstant.DST_IP);
        String DST_PORT = dpiInfo.getString(DpiInfoConstant.DST_PORT);
        String DST_DWMC = dpiInfo.getString(DpiInfoConstant.DST_DWMC);
        String UL_TRAFFIC = dpiInfo.getString(DpiInfoConstant.UL_TRAFFIC);
        String DL_TRAFFIC = dpiInfo.getString(DpiInfoConstant.DL_TRAFFIC);
        String UL_PACKTES = dpiInfo.getString(DpiInfoConstant.UL_PACKTES);
        String DL_PACKETS = dpiInfo.getString(DpiInfoConstant.DL_PACKETS);
        String L4PROTO = dpiInfo.getString(DpiInfoConstant.L4PROTO);
        String PROTOCOL = dpiInfo.getString(DpiInfoConstant.PROTOCOL);
        String MANUFACTURER = dpiInfo.getString(DpiInfoConstant.MANUFACTURER);
        String ASSET_TYPE = dpiInfo.getString(DpiInfoConstant.ASSET_TYPE);
        String START_TIME = dpiInfo.getString(DpiInfoConstant.START_TIME);
        String END_TIME = dpiInfo.getString(DpiInfoConstant.END_TIME);
        String REMARKS = dpiInfo.getString(DpiInfoConstant.REMARKS);
        String HOST = dpiInfo.getString(DpiInfoConstant.HOST);
        String URI = dpiInfo.getString(DpiInfoConstant.URI);
        String DEV_IP = dpiInfo.getString(DpiInfoConstant.DEV_IP);
        String DEV_PORT = dpiInfo.getString(DpiInfoConstant.DEV_PORT);
        String IP_TYPE = dpiInfo.getString(DpiInfoConstant.IP_TYPE);
        String DEV_TYPE = dpiInfo.getString(DpiInfoConstant.DEV_TYPE);
        String DEV_NAME = dpiInfo.getString(DpiInfoConstant.DEV_NAME);
        String SOFT_NAME = dpiInfo.getString(DpiInfoConstant.SOFT_NAME);
        String SOFT_VER = dpiInfo.getString(DpiInfoConstant.SOFT_VER);
        String VENDOR = dpiInfo.getString(DpiInfoConstant.VENDOR);
        String OS = dpiInfo.getString(DpiInfoConstant.OS);
        String OS_VER = dpiInfo.getString(DpiInfoConstant.OS_VER);
        String NET_TYPE = dpiInfo.getString(DpiInfoConstant.NET_TYPE);
        String LAST_TIME = dpiInfo.getString(DpiInfoConstant.LAST_TIME);
        String SRC_MAC = dpiInfo.getString(DpiInfoConstant.SRC_MAC);
        String DST_MAC = dpiInfo.getString(DpiInfoConstant.DST_MAC);
        ps.setString(1,SRC_IP_TYPE);
        ps.setString(2,DST_IP_TYPE);
        ps.setString(3,SRC_IP);
        ps.setString(4,SRC_PORT);
        ps.setString(5,SRC_DWMC);
        ps.setString(6,DST_IP);
        ps.setString(7,DST_PORT);
        ps.setString(8,DST_DWMC);
        ps.setString(9,UL_TRAFFIC);
        ps.setString(10,DL_TRAFFIC);
        ps.setString(11,UL_PACKTES);
        ps.setString(12,DL_PACKETS);
        ps.setString(13,L4PROTO);
        ps.setString(14,PROTOCOL);
        ps.setString(15,MANUFACTURER);
        ps.setString(16,ASSET_TYPE);
        ps.setString(17,START_TIME);
        ps.setString(18,END_TIME);
        ps.setString(19,REMARKS);
        ps.setString(20,HOST);
        ps.setString(21,URI);
        ps.setString(22,DEV_IP);
        ps.setString(23,DEV_PORT);
        ps.setString(24,IP_TYPE);
        ps.setString(25,DEV_TYPE);
        ps.setString(26,DEV_NAME);
        ps.setString(27,SOFT_NAME);
        ps.setString(28,SOFT_VER);
        ps.setString(29,VENDOR);
        ps.setString(30,OS);
        ps.setString(31,OS_VER);
        ps.setString(32,NET_TYPE);
        ps.setString(33,LAST_TIME);
        ps.setString(34,SRC_MAC);
        ps.setString(35,DST_MAC);
        ps.addBatch();
    }

    /**
     * 每条数据的插入都要调用一次 invoke() 方法
     *
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(List<ObjectNode> objectNodes, Context context) throws Exception {
        String date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());

        //遍历数据集合
        for (ObjectNode objectNode : objectNodes) {
            addBatch(objectNode);
        }
        int[] count = ps.executeBatch();//批量后执行
        System.out.println("成功了插入了" + count.length + "行数据");
    }


    private static Connection getConnection(BasicDataSource dataSource) {
        dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
        //注意，替换成自己本地的 mysql 数据库地址和用户名、密码
        dataSource.setUrl("jdbc:mysql://master02:3306/sdc");
        dataSource.setUsername("root");
        dataSource.setPassword("broadtech");
        //设置连接池的一些参数
        dataSource.setInitialSize(10);
        //dataSource.setMaxTotal(50);
        dataSource.setMinIdle(2);

        Connection con = null;
        try {
            con = dataSource.getConnection();
            System.out.println("创建连接池：" + con);
        } catch (Exception e) {
            System.out.println("-----------mysql get connection has exception , msg = " + e.getMessage());
        }
        return con;
    }
}
