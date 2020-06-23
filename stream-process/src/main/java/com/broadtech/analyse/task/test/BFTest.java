package com.broadtech.analyse.task.test;

import org.apache.flink.configuration.Configuration;
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
        streamSource.process(new DeduplicateProcessFunc()).print();
        env.execute("BF test.");
    }
}

class DeduplicateProcessFunc extends ProcessFunction<Integer, Integer>{
    private static final int BF_CARDINAL_THRESHOLD = 100000;
    private static final double BF_FALSE_POSITIVE_RATE = 0.01;
    private volatile BloomFilter<Integer> subOrderFilter;

    @Override
    public void open(Configuration parameters) throws Exception {
        subOrderFilter = BloomFilter.create(Funnels.integerFunnel(), BF_CARDINAL_THRESHOLD, BF_FALSE_POSITIVE_RATE);
    }

    @Override
    public void processElement(Integer value, Context ctx, Collector<Integer> out) throws Exception {
        //判断value是否在BF中
        if(!subOrderFilter.mightContain(value)){
            subOrderFilter.put(value);
            out.collect(value);
        }
    }
}
