package com.broadtech.analyse.flink.window.abnormal;

import com.broadtech.analyse.constants.abnormal.AlarmType;
import com.broadtech.analyse.pojo.abnormal.AlarmResult;
import com.broadtech.analyse.pojo.abnormal.Dpi;
import com.broadtech.analyse.util.TimeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author leo.J
 * @description 同一时间对目的ip同时对多个目的port发送数据，则认为扫描探测
 * @date 2020-08-22 11:29
 */
public class ScanAnalysisWindowFunc extends ProcessWindowFunction<Dpi, AlarmResult, String, TimeWindow> {
    private Integer portCount;

    public ScanAnalysisWindowFunc(Integer portCount) {
        this.portCount = portCount;
    }

    @Override
    public void process(String destIpAddress, Context context, Iterable<Dpi> elements, Collector<AlarmResult> out) throws Exception {
        Set<String> portSet = new HashSet<>();
        List<String> traceIds = new ArrayList<>();
        for(Dpi dpi: elements){
            portSet.add(dpi.getDestPort());
            traceIds.add(dpi.getId());
        }
        if(portSet.size() > portCount){
            AlarmResult alarmResult = new AlarmResult("", destIpAddress,
                    TimeUtils.convertTimestamp2Date(context.window().getStart(), "yyyy-MM-dd hh:mm:ss"),
                    TimeUtils.convertTimestamp2Date(context.window().getEnd(), "yyyy-MM-dd hh:mm:ss"),
                    "",
                    "", "", "", "", "", AlarmType.SCAN.value, StringUtils.join(traceIds, ","));
            out.collect(alarmResult);
        }

    }
}
