package com.broadtech.analyse.flink.source.cmcc;

import com.mysql.cj.jdbc.Driver;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

/**
 * @author leo.J
 * @description 从mysql读取资产标签库
 * @date 2020-04-26 17:16
 */
public class AssetLabelReader extends RichSourceFunction<Map<String, Tuple3<Integer, String, String>>> {
    private static final Logger LOG = LoggerFactory.getLogger(AssetLabelReader.class);

    private Connection connection = null;
    private PreparedStatement ps = null;
    private BasicDataSource dataSource = null;
    private volatile boolean isRunning = true;

    private String jdbcUrl;
    private String userName;
    private String password;

    public AssetLabelReader(String jdbcUrl, String userName, String password) {
        this.jdbcUrl = jdbcUrl;
        this.userName = userName;
        this.password = password;
    }

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
        connection = DriverManager.getConnection(jdbcUrl, userName, password);//获取连接
        String sql = "select l.id as id,l.label_1 as label1,l.label_2 as label2,k.keyword as keyword from label l join label_key k on l.id = k.label_id";
        ps = connection.prepareStatement(sql);

    }

    /**
     * 按天更新
     * @param ctx
     * @throws Exception
     */
    public void run(SourceContext<Map<String, Tuple3<Integer, String, String>>> ctx) throws Exception {
        Map<String, Tuple3<Integer, String, String>> labelMap = new HashMap<>();
        while (isRunning) {
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                int id = rs.getInt("id");
                String label1 = rs.getString("label1");
                String label2 = rs.getString("label2");
                String keyword = rs.getString("keyword");

                labelMap.put(keyword, Tuple3.of(id, label1, label2));
            }
            ctx.collect(labelMap);
            labelMap.clear();
            //设置多久查询更新mysql数据
            Thread.sleep(1 * 24 * 60 * 60 * 1000);
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
