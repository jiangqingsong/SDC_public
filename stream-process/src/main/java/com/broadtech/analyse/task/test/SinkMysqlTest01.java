package com.broadtech.analyse.task.test;

import com.broadtech.analyse.util.env.FlinkUtils;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import java.sql.Types;

/**
 * @author jiangqingsong
 * @description TODO 测试一下
 * @date 2020-06-15 23:07
 */
public class SinkMysqlTest01 {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = FlinkUtils.getEnv();
        DataStreamSource<Tuple2<String, String>> source = env.fromElements(Tuple2.of("AAA", "111"));

        String query = "insert into test_insert(name, id) values(?,?)";
        JDBCOutputFormat jdbcOutput = JDBCOutputFormat.buildJDBCOutputFormat()
                .setDrivername("com.mysql.jdbc.Driver")
                .setDBUrl("jdbc:mysql://master02:3306/sdc?characterEncoding=utf8")
                .setQuery(query)
                .setSqlTypes(new int[]{Types.VARCHAR, Types.VARCHAR})
                .finish();
        SingleOutputStreamOperator<Row> mapedStream = source.map(x -> {
            Row row = new Row(2);
            row.setField(0, x.f0);
            row.setField(1, x.f1);
            return row;
        });

        mapedStream.writeUsingOutputFormat(jdbcOutput);


        env.execute("test insert!");
    }
}
