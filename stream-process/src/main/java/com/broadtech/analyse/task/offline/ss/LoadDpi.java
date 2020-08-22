package com.broadtech.analyse.task.offline.ss;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.MapOperator;

/**
 * @author jiangqingsong
 * @description 离线导入dpi数据
 * @date 2020-08-12 10:12
 */
public class LoadDpi {
    public static void main(String[] args) throws Exception {
        String driverClass = "com.mysql.cj.jdbc.Driver";
        String dbUrl = "jdbc:mysql://192.168.5.93:3306/sdc?useUnicode=true&characterEncoding=utf8&serverTimezone=GMT";
        String userNmae = "root";
        String passWord = "broadtech";
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String sql = "";
        String filePath = "D:\\SDC\\data\\态势感知\\日志数据\\dpi0812.txt";

        MapOperator<String, String> map = env.readTextFile(filePath).map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                String[] split = value.split("|");

                return split[0];
            }
        });
        map.print();
    }
}
