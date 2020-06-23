package com.broadtech.analyse.util.db;

import com.broadtech.analyse.constants.asset.AssetDiscoverConstant;
import com.broadtech.analyse.flink.sink.cmcc.AssetDiscoverMysqlSink;
import org.apache.commons.dbcp.BasicDataSource;

import java.sql.Connection;
import java.util.Properties;

/**
 * @author jiangqingsong
 * @description jdbc工具类
 * @date 2020-06-08 15:39
 */
public class DbUtils {
    public static Connection getCon(BasicDataSource dataSource, String jdbcPro) throws Exception {
        String jdbcUrl;
        String userName;
        String password;
        String driverClass;
        try {
            Properties pp = new Properties();
            pp.load(AssetDiscoverMysqlSink.class.getResourceAsStream(jdbcPro));
            jdbcUrl = pp.getProperty(AssetDiscoverConstant.JDBC_URL);
            userName = pp.getProperty(AssetDiscoverConstant.JDBC_USER);
            password = pp.getProperty(AssetDiscoverConstant.JDBC_PWD);
            driverClass = pp.getProperty(AssetDiscoverConstant.JDBC_DRIVER);
            assert jdbcUrl != null;
            assert userName != null;
            assert password != null;
        } catch (Exception ex) {
            System.out.println("加载数据库配置信息出错,请检查配置：" + jdbcPro);
            ex.printStackTrace();
            throw ex;
        }
        dataSource.setDriverClassName(driverClass);
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
        }
        return con;

    }
}
