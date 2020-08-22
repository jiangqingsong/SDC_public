package com.broadtech.analyse.task.test;

import com.broadtech.analyse.pojo.ss.SecurityLog;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.base.Charsets;
import org.apache.flink.shaded.guava18.com.google.common.hash.BloomFilter;
import org.apache.flink.shaded.guava18.com.google.common.hash.Funnel;
import org.apache.flink.shaded.guava18.com.google.common.hash.Funnels;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;


/**
 * @author jiangqingsong
 * @description 布隆过滤器demo
 * @date 2020-06-02 13:44
 */
public class BFTest {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Integer> streamSource = env.fromElements(1, 2, 3, 1, 4, 6, 6);

        SecurityLog s1 = new SecurityLog();
        s1.setDestport("p1");
        s1.setEventname("e1");
        SecurityLog s2 = new SecurityLog();
        s2.setDestport("p2");
        s2.setEventname("e2");
        SecurityLog s3 = new SecurityLog();
        s3.setDestport("p3");
        s3.setEventname("e3");
        DataStreamSource<SecurityLog> securityStream = env.fromElements(s1, s1, s2, s2, s3, s3);
        securityStream.process(new DeduplicateProcessFunc()).print();
        env.execute("BF test.");
    }
}

class DeduplicateProcessFunc extends ProcessFunction<SecurityLog, SecurityLog>{
    private static final int BF_CARDINAL_THRESHOLD = 100000;
    private static final double BF_FALSE_POSITIVE_RATE = 0.01;
    private volatile BloomFilter<String> subOrderFilter;

    @Override
    public void open(Configuration parameters) throws Exception {
        subOrderFilter = BloomFilter.create(Funnels.stringFunnel(Charsets.UTF_8), BF_CARDINAL_THRESHOLD, BF_FALSE_POSITIVE_RATE);
    }

    @Override
    public void processElement(SecurityLog value, Context ctx, Collector<SecurityLog> out) throws Exception {
        //判断value是否在BF中
        String checkStr = value.getEventname() + value.getDestport();
        if(!subOrderFilter.mightContain(checkStr)){
            subOrderFilter.put(checkStr);
            out.collect(value);
        }
    }
}
