package com.broadtech.analyse.util;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;

/**
 * @author jiangqingsong
 * @description
 * @date 2020-05-15 15:40
 */
public class TimeUtils {
    public static long getTimestamp(String pattern, String time){
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        TemporalAccessor parse = formatter.parse(time);
        long timestamp = LocalDateTime.from(parse).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        return timestamp;
    }
}
