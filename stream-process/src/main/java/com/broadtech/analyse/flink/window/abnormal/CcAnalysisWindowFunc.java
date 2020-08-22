package com.broadtech.analyse.flink.window.abnormal;

import com.broadtech.analyse.constants.abnormal.AlarmType;
import com.broadtech.analyse.pojo.abnormal.AlarmResult;
import com.broadtech.analyse.pojo.abnormal.Dpi;
import com.broadtech.analyse.util.TimeUtils;
import org.apache.commons.collections.ListUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * @author jiangqingsong
 * @description C&C异常分析
 * @date 2020-08-21 18:01
 */
public class CcAnalysisWindowFunc extends ProcessWindowFunction<Dpi, AlarmResult, String, TimeWindow> {
    private static Logger LOG = Logger.getLogger(CcAnalysisWindowFunc.class);
    private Integer srcIpCount;
    public CcAnalysisWindowFunc(Integer srcIpCount) {
        this.srcIpCount = srcIpCount;
    }

    @Override
    public void process(String key, Context context, Iterable<Dpi> input, Collector<AlarmResult> out) throws Exception {
        Set<String> srcIpSet = new HashSet<>();
        for(Dpi dpi: input){
            srcIpSet.add(dpi.getSrcIPAddress());
        }
        if(srcIpSet.size() > srcIpCount){
            AlarmResult alarmResult = new AlarmResult("", key,
                    TimeUtils.convertTimestamp2Date(context.window().getStart(), "yyyy-MM-dd hh:mm:ss"),
                    TimeUtils.convertTimestamp2Date(context.window().getEnd(), "yyyy-MM-dd hh:mm:ss"),
                    "", "",
                    "", "", "", "", AlarmType.CC.value);
            out.collect(alarmResult);
        }
    }
}
