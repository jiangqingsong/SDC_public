package com.broadtech.analyse.flink.source;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mysql.cj.jdbc.Driver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

/**
 * @author leo.J
 * @description 读取mysql数据作为datasource
 * @date 2020-04-26 17:16
 */
public class JdbcReader extends RichSourceFunction<Map<Integer, Tuple2<Double, Double>>> {
    private static final Logger LOG = LoggerFactory.getLogger(JdbcReader.class);

    private Connection connection = null;
    private PreparedStatement ps = null;
    private volatile boolean isRunning = true;

    /**
     * 打开jdbc连接
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        DriverManager.registerDriver(new Driver());
        //String db_url = "jdbc:mysql://master02:3306/sdc";
        String db_url = "jdbc:mysql://localhost:3306/sdc?useUnicode=true&characterEncoding=utf8&serverTimezone=GMT";
        //connection = DriverManager.getConnection(db_url, "root", "broadtech");//获取连接
        connection = DriverManager.getConnection(db_url, "root", "123456");//获取连接
        String sql = "select * from abnormal_traffic_bound";//todo  根据日期来查询
        ps = connection.prepareStatement(sql);

    }

    /**
     * 查询结果处理，每五分钟更新一次
     *
     * @param ctx
     * @throws Exception
     */
    public void run(SourceContext<Map<Integer, Tuple2<Double, Double>>> ctx) throws Exception {
        Map<Integer, Tuple2<Double, Double>> abnormalTrafficMap = new HashMap<>();
        while (isRunning) {
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                //String date = rs.getString("date");
                Integer hour = Integer.valueOf(rs.getString("hour"));
                Double dBound = Double.valueOf(rs.getString("d_bound"));
                Double uBound = Double.valueOf(rs.getString("u_bound"));
                abnormalTrafficMap.put(hour, new Tuple2<Double, Double>(dBound, uBound));
            }

            ctx.collect(abnormalTrafficMap);
            abnormalTrafficMap.clear();

            //设置多久查询更新mysql数据
            Thread.sleep(1 * 10 * 1000);
        }
    }

    public void cancel() {
        try {
            super.close();
            if (connection != null) {
                connection.close();
            }
            if (ps != null) {
                ps.close();
            }
        } catch (Exception e) {
            LOG.error("runException{}", e);
        }
        isRunning = false;
    }
}
