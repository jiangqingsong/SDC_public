package com.broadtech.analyse.flink.watermark;

import com.broadtech.analyse.pojo.traffic.Traffic;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.sql.Timestamp;

/**
 * @author jiangqingsong
 * @description
 * @date 2020-04-26 18:05
 */
public class TrafficAssignTimestampAndWatermarks extends BoundedOutOfOrdernessTimestampExtractor<Traffic> {
    public TrafficAssignTimestampAndWatermarks(Time maxOutOfOrderness) {
        super(maxOutOfOrderness);
    }

    @Override
    public long extractTimestamp(Traffic element) {
        //2020-04-09 17:03:19
        long evenTime = Timestamp.valueOf(element.getTime()).getTime();
        return evenTime;
    }

}
