package com.broadtech.analyse.flink.sink.abnormal;

import com.broadtech.analyse.pojo.abnormal.AlarmResult;
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
public class AlarmResultSinkBak extends RichSinkFunction<List<AlarmResult>> {
    private static Logger LOG = Logger.getLogger(AlarmResultSink.class);
    private PreparedStatement ps;
    private BasicDataSource dataSource;
    private Connection connection;
    private String jdbcUrl;
    private String userName;
    private String password;

    public AlarmResultSinkBak(String jdbcUrl, String userName, String password) {
        this.jdbcUrl = jdbcUrl;
        this.userName = userName;
        this.password = password;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        dataSource = new BasicDataSource();
        connection = getCon(dataSource);
        String sql = "insert into  sdc.telecom_alarm_dpi(src_ip_address, dest_ip_address, window_start, window_end," +
                "traffic_size, file_name, keyword, geo, category, score, alarm_type, )" +
                "values (?,?,?,?,?,?,?,?,?,?,?);";
        ps = this.connection.prepareStatement(sql);
    }

    @Override
    public void invoke(List<AlarmResult> value, Context context) throws Exception {
        for(AlarmResult alarmResult: value){
            addBatch(alarmResult);
        }
        int[] count = ps.executeBatch();//批量后执行
        System.out.println("telecom_alarm_dpi 成功了插入了" + count.length + "行数据");
        LOG.info("telecom_alarm_dpi 成功了插入了" + count.length + "行数据");
    }
    public void addBatch(AlarmResult alarmResult) throws SQLException {
        ps.setString(1, alarmResult.getSrcIpAddress());
        ps.setString(2, alarmResult.getDestIpAddress());
        ps.setString(3, alarmResult.getWindowStart());
        ps.setString(4, alarmResult.getWindowEnd());
        ps.setString(5, alarmResult.getTrafficSize());
        ps.setString(6, alarmResult.getFileName());
        ps.setString(7, alarmResult.getKeyword());
        ps.setString(8, alarmResult.getGeo());
        ps.setString(9, alarmResult.getCategory());
        ps.setString(10, alarmResult.getScore());
        ps.setInt(11, alarmResult.getAlarmType());
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
