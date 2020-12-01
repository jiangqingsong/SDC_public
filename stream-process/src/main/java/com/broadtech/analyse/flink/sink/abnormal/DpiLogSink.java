package com.broadtech.analyse.flink.sink.abnormal;

import com.broadtech.analyse.pojo.abnormal.Dpi;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

/**
 * @author leo.J
 * @description 告警信息入库
 * @date 2020-08-22 11:18
 */
public class DpiLogSink extends RichSinkFunction<List<Dpi>> {
    private static Logger LOG = Logger.getLogger(DpiLogSink.class);
    private PreparedStatement ps;
    private BasicDataSource dataSource;
    private Connection connection;
    private String jdbcUrl;
    private String userName;
    private String password;

    public DpiLogSink(String jdbcUrl, String userName, String password) {
        this.jdbcUrl = jdbcUrl;
        this.userName = userName;
        this.password = password;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        dataSource = new BasicDataSource();
        connection = getCon(dataSource);
        String sql = "insert into  sdc.telecom_dpi_log(srcI_ip_address,src_port,dest_ip_address,dest_port,protocol,start_time," +
                "end_time,connect_duration,packet_size,packet_number,upstream_traffic,downstream_traffic,traffic_size,domain_name,url,file_name,id)" +
                "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);";
        ps = this.connection.prepareStatement(sql);
    }

    @Override
    public void invoke(List<Dpi> value, Context context) throws Exception {
        for(Dpi dpi: value){
            addBatch(dpi);
        }
        int[] count = ps.executeBatch();//批量后执行
        LOG.info("telecom_dpi_log 成功了插入了" + count.length + "行数据");
    }
    public void addBatch(Dpi dpi) throws SQLException {
        ps.setString(1, dpi.getSrcIPAddress());
        ps.setString(2, dpi.getSrcPort());
        ps.setString(3, dpi.getDestIPAddress());
        ps.setString(4, dpi.getDestPort());
        ps.setString(5, dpi.getProtocol());
        ps.setString(6, dpi.getStartTime());
        ps.setString(7, dpi.getEndTime());
        ps.setString(8, dpi.getConnectDuration());
        ps.setString(9, dpi.getPacketSize());
        ps.setString(10, dpi.getPacketNumber());
        ps.setString(11, dpi.getUpstreamTraffic());
        ps.setString(12, dpi.getDownstreamTraffic());
        ps.setString(13, dpi.getTrafficSize());
        ps.setString(14, dpi.getDomainName());
        ps.setString(15, dpi.getUrl());
        ps.setString(16, dpi.getFileName());
        ps.setString(17, dpi.getId());
        ps.addBatch();
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
