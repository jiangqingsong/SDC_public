package com.broadtech.analyse.flink.process.abnormal;

import com.broadtech.analyse.constants.abnormal.AlarmType;
import com.broadtech.analyse.pojo.abnormal.AlarmResult;
import com.broadtech.analyse.pojo.abnormal.Dpi;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author leo.J
 * @description 内网失陷主机检测
 * @date 2020-08-22 14:17
 */
public class IntranetFallProcess extends ProcessFunction<Dpi, AlarmResult> {
    private Double trafficUpperLimit;

    public IntranetFallProcess(Double trafficUpperLimit) {
        this.trafficUpperLimit = trafficUpperLimit;
    }

    @Override
    public void processElement(Dpi dpi, Context ctx, Collector<AlarmResult> out) throws Exception {
        Double totalTraffic = Double.valueOf(dpi.getTrafficSize());
        String traceIds = dpi.getId();
        if(totalTraffic > trafficUpperLimit){
            AlarmResult alarmResult = new AlarmResult(dpi.getSrcIPAddress(), dpi.getDestIPAddress(), "", "", dpi.getTrafficSize(), dpi.getFileName(),
                    "", "", "", "", AlarmType.INTRANET_FALL.value, traceIds);
            out.collect(alarmResult);
        }
    }
}
