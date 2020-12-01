package com.broadtech.analyse.flink.process.main;

import com.broadtech.analyse.pojo.ss.SecurityLog;
import org.apache.flink.api.common.functions.FilterFunction;

/**
 * @author leo.J
 * @description
 * @date 2020-10-05 10:09
 */
public class AlarmEvenLogFilter implements FilterFunction<SecurityLog> {
    /**
     * 当secondeventtype、eventname、eventgrade都不为空的时候判定该条日志为告警日志
     * @param value
     * @return
     * @throws Exception
     */
    @Override
    public boolean filter(SecurityLog value) throws Exception {
        //String secondeventtype = value.getSecondeventtype();
        String eventname = value.getEventname();
        String eventgrade = value.getEventgrade();

        return /*!secondeventtype.isEmpty() &&*/ !eventname.isEmpty() && !eventgrade.isEmpty();
    }
}
