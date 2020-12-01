package com.broadtech.analyse.constants.abnormal;

/**
 * @author leo.J
 * @description
 * @date 2020-08-07 17:14
 */
public enum AlarmType {
    /**
     * 告警类型 1-C&C异常外联 2-可疑数据外传 3-扫描探测 4-内网失陷主机检测
     */
    CC(1),
    SUSPICIOUS(2),
    SCAN(3),
    INTRANET_FALL(4);

    public Integer value;
    AlarmType(Integer value) {
        this.value = value;
    }
}
