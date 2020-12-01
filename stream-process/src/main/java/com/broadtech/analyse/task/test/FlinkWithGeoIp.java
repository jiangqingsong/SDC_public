package com.broadtech.analyse.task.test;

import com.broadtech.analyse.util.IPUtils;
import com.broadtech.analyse.util.env.FlinkUtils;
import com.maxmind.geoip2.DatabaseReader;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * @author leo.J
 * @description
 * @date 2020-06-07 10:23
 */
public class FlinkWithGeoIp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = FlinkUtils.getEnv();
        List<String> ipList = new ArrayList<>();
        ipList.add("61.137.125.155");
        ipList.add("113.204.226.35");
        ipList.add("192.168.5.92");
        DataStreamSource<String> ipSource = env.fromCollection(ipList);
        SingleOutputStreamOperator<String> mapedStream = ipSource.map(new RichMapFunction<String, String>() {
            DatabaseReader build;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                String path = "D:\\SDC\\data\\CMCC\\geoip\\GeoLite2-City.mmdb";
                build = new DatabaseReader.Builder(new File(path)).build();
            }

            @Override
            public String map(String ip) throws Exception {

                String city = IPUtils.getCity(build, ip);
                String province = IPUtils.getProvince(build, ip);
                String country = IPUtils.getCountry(build, ip);

                return city + "," + province + "," + country;
            }
        });
        mapedStream.print();

        env.execute("geoip test");
    }
}
