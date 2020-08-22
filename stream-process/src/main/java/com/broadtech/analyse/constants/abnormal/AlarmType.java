package com.broadtech.analyse.constants.abnormal;

/**
 * @author jiangqingsong
 * @description
 * @date 2020-08-07 17:14
 */
public enum AlarmType {
    CC(1),
    SUSPICIOUS(2),
    SCAN(3),
    INTRANET_FALL(4);

    public Integer value;
    AlarmType(Integer value) {
        this.value = value;
    }
}
