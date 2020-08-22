package com.broadtech.analyse.flink.function.common;

import org.apache.flink.shaded.curator.org.apache.curator.shaded.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

/**
 * @author jiangqingsong
 * @description  多条记录转一条放入List中
 * @date 2020-08-12 17:13
 */
public class BatchProcess<IN, OUT> implements AllWindowFunction<IN, OUT, TimeWindow> {
    private static final Logger LOG = LoggerFactory.getLogger(BatchProcess.class);
    @Override
    public void apply(TimeWindow window, Iterable<IN> values, Collector<OUT> out) throws Exception {
        try {
            ArrayList<IN> ins = Lists.newArrayList(values);
            OUT objectNodes = (OUT) ins;
            if (ins.size() > 0) {
                out.collect(objectNodes);
            }
        }catch (Exception e){
            LOG.error("类型转换失败！");
        }
    }
}
