package com.broadtech.analyse.flink.watermark;

import com.broadtech.analyse.pojo.gateway.GatewaySession;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.sql.Timestamp;

/**
 * @author leo.J
 * @description
 * @date 2020-04-26 18:05
 */
public class GatewayAssignTimestampAndWatermarks extends BoundedOutOfOrdernessTimestampExtractor<GatewaySession> {
    public GatewayAssignTimestampAndWatermarks(Time maxOutOfOrderness) {
        super(maxOutOfOrderness);
    }

    @Override
    public long extractTimestamp(GatewaySession element) {
        long evenTime = Timestamp.valueOf(element.getProcedureStarttime()).getTime();
        return evenTime;
    }
}
