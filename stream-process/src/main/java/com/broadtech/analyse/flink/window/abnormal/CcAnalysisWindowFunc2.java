package com.broadtech.analyse.flink.window.abnormal;

import com.broadtech.analyse.constants.abnormal.AlarmType;
import com.broadtech.analyse.pojo.abnormal.AlarmResult;
import com.broadtech.analyse.pojo.abnormal.Dpi;
import com.broadtech.analyse.util.TimeUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;

/**
 * @author jiangqingsong
 * @description C&C异常分析 2：将所述可疑IP对中同一目的IP对应的源IP个数小于或者等于3的目的IP加入到所述可疑目的IP列表
 * @date 2020-08-21 18:01
 */
public class CcAnalysisWindowFunc2 extends ProcessWindowFunction<Dpi, AlarmResult, Tuple2<String, String>, TimeWindow> {
    private static Logger LOG = Logger.getLogger(CcAnalysisWindowFunc2.class);
    private Integer abnormalTimes;
    private Integer abnormalTrafficSize;
    public CcAnalysisWindowFunc2(Integer abnormalTimes, Integer abnormalTrafficSize) {
        this.abnormalTimes = abnormalTimes;
        this.abnormalTrafficSize = abnormalTrafficSize;
    }

    @Override
    public void process(Tuple2<String, String> key, Context context, Iterable<Dpi> input, Collector<AlarmResult> out) throws Exception {
        double totalTrafficSize = 0.0;
        double totalUpStreamTraffic = 0.0;
        double totalDownStreamTraffic = 0.0;

        for(Dpi dpi: input){
            Double trafficSize = Double.valueOf(dpi.getTrafficSize());
            Double upStreamTraffic = Double.valueOf(dpi.getUpstreamTraffic());
            Double downStreamTraffic = Double.valueOf(dpi.getDownstreamTraffic());

            totalTrafficSize += trafficSize;
            totalUpStreamTraffic += upStreamTraffic;
            totalDownStreamTraffic += totalDownStreamTraffic;
        }

        //目的IP和源IP之间传输的数据包个数大于1000 || 目的IP和源IP之间传输的上行流量大于下行流量N倍
        if(totalTrafficSize >= abnormalTrafficSize || totalUpStreamTraffic / totalDownStreamTraffic <= abnormalTimes){
            String srcIpAddress = key.f0;
            String destIpAddress = key.f1;
            String windowStart = TimeUtils.convertTimestamp2Date(context.window().getStart(), "yyyy-MM-dd hh:mm:ss");
            String windowEnd = TimeUtils.convertTimestamp2Date(context.window().getEnd(), "yyyy-MM-dd hh:mm:ss");
            out.collect(new AlarmResult(srcIpAddress, destIpAddress, windowStart, windowEnd, String.valueOf(totalTrafficSize),
                    "", "", "", "", "", AlarmType.CC.value));
        }
    }
}
