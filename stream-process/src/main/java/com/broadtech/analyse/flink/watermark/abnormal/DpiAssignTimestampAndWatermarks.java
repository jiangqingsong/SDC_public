package com.broadtech.analyse.flink.watermark.abnormal;

import com.broadtech.analyse.pojo.abnormal.Dpi;
import com.broadtech.analyse.pojo.gateway.GatewaySession;
import com.broadtech.analyse.util.TimeUtils;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author jiangqingsong
 * @description
 * @date 2020-08-22 13:45
 */
public class DpiAssignTimestampAndWatermarks extends BoundedOutOfOrdernessTimestampExtractor<Dpi> {
    private static final String TIME_PATTERN = "yyyy-MM-dd HH:mm:ss";

    public DpiAssignTimestampAndWatermarks(Time maxOutOfOrderness) {
        super(maxOutOfOrderness);
    }
    @Override
    public long extractTimestamp(Dpi dpi) {
        return TimeUtils.getTimestamp(TIME_PATTERN, dpi.getStartTime());
    }
}
