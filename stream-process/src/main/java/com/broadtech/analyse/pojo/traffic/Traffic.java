package com.broadtech.analyse.pojo.traffic;

/**
 * @author leo.J
 * @description
 * @date 2020-04-26 17:57
 */
public class Traffic {
    private String key;
    private String time;
    private Double totalTraffic;
    private Double abnormalTraffic;
    private Double normalTraffic;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public Double getTotalTraffic() {
        return totalTraffic;
    }

    public void setTotalTraffic(Double totalTraffic) {
        this.totalTraffic = totalTraffic;
    }

    public Double getAbnormalTraffic() {
        return abnormalTraffic;
    }

    public void setAbnormalTraffic(Double abnormalTraffic) {
        this.abnormalTraffic = abnormalTraffic;
    }

    public Double getNormalTraffic() {
        return normalTraffic;
    }

    public void setNormalTraffic(Double normalTraffic) {
        this.normalTraffic = normalTraffic;
    }
}
