package com.broadtech.analyse.flink.sink.ss;

import com.broadtech.analyse.constants.ss.EvenAlarmConstant;
import com.broadtech.analyse.pojo.cmcc.AssetScanOrigin;
import com.broadtech.analyse.pojo.ss.SecurityLog;
import com.broadtech.analyse.pojo.ss.ThreatIntelligence2;
import com.broadtech.analyse.util.TimeUtils;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
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
 * @date 2020-08-11 11:11
 */
public class EventAlarm2MysqlSink extends RichSinkFunction<List<Tuple5<SecurityLog, Integer, Long, String, ThreatIntelligence2>>> {
    private static Logger LOG = Logger.getLogger(EventAlarm2MysqlSink.class);
    private PreparedStatement ps;
    private BasicDataSource dataSource;
    private Connection connection;
    private String jdbcUrl;
    private String userName;
    private String password;

    public EventAlarm2MysqlSink(String jdbcUrl, String userName, String password) {
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
        String sql = "insert into  " + EvenAlarmConstant.TAB_NAME_EVEN_ALARM + "(id,eventname,firsteventtype,secondeventtype," +
                "thirdeventtype,eventgrade,discoverytime,alarmtimes,deviceipaddress,devicetype,devicefactory,devicemodel," +
                "devicename,srcipaddress,srcport,srcmacaddress,destipaddress,destport,affectdevice,eventdesc,attackersourcetype," +
                "attackersource,threattype,threatgeo,threatscore,windowendtime,vulnerabilitynumber)" +
                "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);";
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


    public void addBatch(Tuple2<AssetScanOrigin, String> tuple) throws SQLException {
        AssetScanOrigin scan = tuple.f0;
        String number = tuple.f1;
        ps.setString(1, scan.getResourceName());
        ps.addBatch();
    }

    /**
     * 每条数据的插入都要调用一次 invoke() 方法
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(List<Tuple5<SecurityLog, Integer, Long, String, ThreatIntelligence2>> tuples, Context context) throws Exception {
        //遍历数据集合
        for(Tuple5<SecurityLog, Integer, Long, String, ThreatIntelligence2> tuple: tuples){
            addBatch(tuple);
        }
        int[] count = ps.executeBatch();//批量后执行
        System.out.println("成功了插入了" + count.length + "行数据");
        LOG.info("成功了插入了" + count.length + "行数据");
    }
    public void addBatch(Tuple5<SecurityLog, Integer, Long, String, ThreatIntelligence2> tuple) throws SQLException {
        SecurityLog securityLog = tuple.f0;
        Integer count = tuple.f1;
        Long windowEnd = tuple.f2;
        String number = tuple.f3;
        ThreatIntelligence2 threatIntelligence2 = tuple.f4;
        ps.setInt(1, Integer.valueOf(securityLog.getId()));
        ps.setString(2, securityLog.getEventname());
        ps.setString(3, securityLog.getFirsteventtype());
        ps.setString(4, securityLog.getSecondeventtype());
        ps.setString(5, securityLog.getThirdeventtype());
        ps.setString(6, securityLog.getEventgrade());
        ps.setString(7, securityLog.getEventgeneratetime());
        ps.setString(8, count.toString());
        ps.setString(9, securityLog.getDeviceipaddress());
        ps.setString(10, securityLog.getDevicetype());
        ps.setString(11, securityLog.getDevicefactory());
        ps.setString(12, securityLog.getDevicemodel());
        ps.setString(13, securityLog.getDevicename());
        ps.setString(14, securityLog.getSrcipaddress());
        ps.setString(15, securityLog.getSrcport());
        ps.setString(16, securityLog.getSrcmacaddress());
        ps.setString(17, securityLog.getDestipaddress());
        ps.setString(18, securityLog.getDestport());
        ps.setString(19, "");
        ps.setString(20, securityLog.getEventdesc());

        ps.setString(21, threatIntelligence2.getType());
        ps.setString(22, threatIntelligence2.getValue());
        ps.setString(23, threatIntelligence2.getCategory());
        ps.setString(24, threatIntelligence2.getGeo());
        ps.setString(25, threatIntelligence2.getScore());
        ps.setString(26, TimeUtils.convertTimestamp2Date(windowEnd, "yyyy-MM-dd hh:mm:ss"));
        ps.setString(27, number);
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
