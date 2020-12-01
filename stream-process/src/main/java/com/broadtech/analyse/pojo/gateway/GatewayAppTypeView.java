package com.broadtech.analyse.pojo.gateway;

/**
 * @author leo.J
 * @description
 * @date 2020-05-11 16:47
 */
public class GatewayAppTypeView {
    private String appType;
    private Double allTraffic ;
    private long windowEnd ;

    public GatewayAppTypeView(String appType, Double allTraffic, long windowEnd) {
        this.appType = appType;
        this.allTraffic = allTraffic;
        this.windowEnd = windowEnd;
    }

    public long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(long windowEnd) {
        this.windowEnd = windowEnd;
    }

    public String getAppType() {
        return appType;
    }

    public void setAppType(String appType) {
        this.appType = appType;
    }

    public Double getAllTraffic() {
        return allTraffic;
    }

    public void setAllTraffic(Double allTraffic) {
        this.allTraffic = allTraffic;
    }
}
