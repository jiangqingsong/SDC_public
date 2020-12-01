package com.broadtech.analyse.flink.sink.cmcc;

import com.alibaba.fastjson.JSON;
import com.broadtech.analyse.constants.asset.AssetConstants;
import com.broadtech.analyse.constants.asset.ScanCollectConstant;
import com.broadtech.analyse.pojo.cmcc.AssetScanOrigin;
import com.broadtech.analyse.util.TimeUtils;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

/**
 * @author leo.J
 * @description Agent采集资产数据mysql
 * @date 2020-05-28 15:37
 */
public class AssetScanWithVulnerMysqlSink extends RichSinkFunction<List<Tuple2<AssetScanOrigin, String>>> {
    private static Logger LOG = Logger.getLogger(AssetScanWithVulnerMysqlSink.class);
    private PreparedStatement ps;
    private BasicDataSource dataSource;
    private Connection connection;
    private String jdbcUrl;
    private String userName;
    private String password;

    public AssetScanWithVulnerMysqlSink(String jdbcUrl, String userName, String password) {
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
        String[] numbers = number.split(",");
        for(int i=0; i<numbers.length; i++){
            ps.setString(1, scan.getDeviceIPAddress());
            ps.setString(2, numbers[i]);
            ps.setString(3, "");//没有绿盟的漏洞信息
            ps.setInt(4, 1);//1代表漏洞来源CNVD
            ps.setString(5, TimeUtils.convertTimestamp2Date(System.currentTimeMillis(), "yyyy-MM-dd hh:mm:ss"));//1代表漏洞来源CNVD
            ps.addBatch();
        }

    }

    /**
     * 每条数据的插入都要调用一次 invoke() 方法
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(List<Tuple2<AssetScanOrigin, String>> tuples, Context context) throws Exception {
        dataSource = new BasicDataSource();
        connection = getCon(dataSource);

        String sql = "insert into " + AssetConstants.TAB_NAME_LVULNERABILITY + "(ip, cnvd_number, lm_number, source, insert_time)values(?,?,?,?,?);";
        ps = this.connection.prepareStatement(sql);
        //遍历数据集合
        for (Tuple2<AssetScanOrigin, String> tuple : tuples) {
            addBatch(tuple);
        }
        try {
            int[] count = ps.executeBatch();//批量后执行
            LOG.info(ScanCollectConstant.TAB_NAME_VULNERABILITY + ": 成功了插入了" + count.length + "行数据");
            System.out.println(ScanCollectConstant.TAB_NAME_VULNERABILITY + ": 成功了插入了" + count.length + "行数据");
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
