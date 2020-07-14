package com.broadtech.analyse.task.test;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.broadtech.analyse.util.env.FlinkUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;

/**
 * @author jiangqingsong
 * @description Test: 使用druid连接池测试长时间不使用连接，
 * @date 2020-06-23 14:37
 */
public class DruidForMysqlTest {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = FlinkUtils.getEnv();

        List<Tuple2<Integer, String>> list = new ArrayList<>();
        list.add(Tuple2.of(1, "A"));
        list.add(Tuple2.of(2, "B"));
        list.add(Tuple2.of(3, "C"));
        list.add(Tuple2.of(4, "D"));
        DataStreamSource<Tuple2<Integer, String>> streamSource = env.fromCollection(list);

        streamSource.addSink(new RichSinkFunction<Tuple2<Integer, String>>() {
            private static final long serialVersionUID = 20000L;
            private String driver = "";
            private String url = "jdbc:mysql://master02:3306/sdc?characterEncoding=utf8";
            private String username = "root";
            private String password = "broadtech";
            private DruidDataSource dataSource;
            private PreparedStatement statement;
            private DruidPooledConnection connection;


            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                getConnect();
                connection = dataSource.getConnection();
                statement = connection.prepareStatement("insert into test_druid(id,name)values(?,?)");
            }
            @Override
            public void invoke(Tuple2<Integer, String> value, Context context) throws Exception {
                statement.setInt(1, value.f0);
                statement.setString(2, value.f1);
                int size = statement.executeUpdate();
                System.out.println("insert size: " + size);
            }
            @Override
            public void close() throws Exception {
                dataSource.close();
                statement.close();
                connection.close();
            }

            public void getConnect(){
                /*dataSource = new DruidDataSource();
                dataSource.setDriverClassName("com.mysql.jdbc.Driver");
                dataSource.setUrl("jdbc:mysql://master02:3306/sdc?characterEncoding=utf8");
                dataSource.setUsername("root");
                dataSource.setPassword("broadtech");
                dataSource.setInitialSize(5);   //初始化时建立物理连接的个数。初始化发生在显示调用init方法，或者第一次getConnection时
                dataSource.setMinIdle(10);  //最小连接池数量
                dataSource.setMaxActive(5);  //最大连接池数量
                dataSource.setMaxWait(1000 * 20); //获取连接时最大等待时间，单位毫秒。配置了maxWait之后，缺省启用公平锁，并发效率会有所下降，如果需要可以通过配置useUnfairLock属性为true使用非公平锁。
                dataSource.setTimeBetweenEvictionRunsMillis(1000 * 60);  //有两个含义：1) Destroy线程会检测连接的间隔时间2) testWhileIdle的判断依据，详细看testWhileIdle属性的说明
                dataSource.setMaxEvictableIdleTimeMillis(1000 * 60 * 60 * 3);  //<!-- 配置一个连接在池中最大生存的时间，单位是毫秒 -->
                dataSource.setMinEvictableIdleTimeMillis(1000 * 60 * 60 * 1);  //<!-- 配置一个连接在池中最小生存的时间，单位是毫秒 -->
                dataSource.setTestWhileIdle(true);  // <!-- 这里建议配置为TRUE，防止取到的连接不可用 -->
                dataSource.setTestOnBorrow(true);
                dataSource.setTestOnReturn(false);
                dataSource.setValidationQuery("select 1");*/

                dataSource = new DruidDataSource();
                dataSource.setDriverClassName(driver);
                dataSource.setUrl(url);
                dataSource.setUsername(username);
                dataSource.setPassword(password);
                dataSource.setValidationQuery("select 1");
            }
        });


        env.execute("test druid for mysql.");
    }
}
