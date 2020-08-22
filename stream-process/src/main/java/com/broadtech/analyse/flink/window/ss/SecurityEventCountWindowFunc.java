package com.broadtech.analyse.flink.window.ss;

import com.broadtech.analyse.pojo.ss.SecurityLog;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author jiangqingsong
 * @description discard!!!
 * @date 2020-08-17 15:56
 */
public class SecurityEventCountWindowFunc implements WindowFunction<Integer, Tuple2<SecurityLog, Integer>, Tuple5<String, String, String, String, String>, TimeWindow> {
    @Override
    public void apply(Tuple5<String, String, String, String, String> key,
                      TimeWindow window, Iterable<Integer> input, Collector<Tuple2<SecurityLog, Integer>> out) throws Exception {
    }
}
