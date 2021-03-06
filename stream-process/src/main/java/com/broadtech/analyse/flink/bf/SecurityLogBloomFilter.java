package com.broadtech.analyse.flink.bf;

import com.broadtech.analyse.pojo.ss.SecurityLog;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.base.Charsets;
import org.apache.flink.shaded.guava18.com.google.common.hash.BloomFilter;
import org.apache.flink.shaded.guava18.com.google.common.hash.Funnels;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;

/**
 * @author leo.J
 * @description 按照原始字段某几个字段去重
 * @date 2020-08-04 11:43
 */
public class SecurityLogBloomFilter extends ProcessFunction<Tuple2<SecurityLog, Integer>, Tuple2<SecurityLog, Integer>> {
    private static Logger LOG = Logger.getLogger(SecurityLogBloomFilter.class);
    private static final int BF_CARDINAL_THRESHOLD = 100000;
    private static final double BF_FALSE_POSITIVE_RATE = 0.01;
    private volatile BloomFilter<String> subOrderFilter;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        subOrderFilter = BloomFilter.create(Funnels.stringFunnel(Charsets.UTF_8), BF_CARDINAL_THRESHOLD, BF_FALSE_POSITIVE_RATE);
    }

    @Override
    public void processElement(Tuple2<SecurityLog, Integer> tuple2, Context context, Collector<Tuple2<SecurityLog, Integer>> out) throws Exception {
        StringBuilder distinctKey = new StringBuilder();
        SecurityLog securityLog = tuple2.f0;
        String deviceipaddress = securityLog.getDeviceipaddress();
        String eventname = securityLog.getEventname();
        String firsteventtype = securityLog.getFirsteventtype();
        String secondeventtype = securityLog.getSecondeventtype();
        String thirdeventtype = securityLog.getThirdeventtype();
        distinctKey.append(deviceipaddress);
        distinctKey.append(eventname);
        distinctKey.append(firsteventtype);
        distinctKey.append(secondeventtype);
        distinctKey.append(thirdeventtype);
        //判断value是否在BF中
        if(!subOrderFilter.mightContain(distinctKey.toString())){
            subOrderFilter.put(distinctKey.toString());
            out.collect(Tuple2.of(securityLog, tuple2.f1));
        }
    }
}
