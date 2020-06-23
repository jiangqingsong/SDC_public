package com.broadtech.analyse.pojo.gateway;

/**
 * @author jiangqingsong
 * @description 用户行为聚合结果类
 * @date 2020-05-09 17:13
 */
public class GatewayUserAction {
    private String appType;
    private Double appTypeTraffic;
    private String appSubType;
    private Double appSubTypeTraffic;
    private Double totalTraffic;
    private Long totalUsers;
    private String appStatu;
    private Long appStatuCnt;
    private Long appStatuAllCnt;

    public String getAppType() {
        return appType;
    }

    public void setAppType(String appType) {
        this.appType = appType;
    }

    public Double getAppTypeTraffic() {
        return appTypeTraffic;
    }

    public void setAppTypeTraffic(Double appTypeTraffic) {
        this.appTypeTraffic = appTypeTraffic;
    }

    public String getAppSubType() {
        return appSubType;
    }

    public void setAppSubType(String appSubType) {
        this.appSubType = appSubType;
    }

    public Double getAppSubTypeTraffic() {
        return appSubTypeTraffic;
    }

    public void setAppSubTypeTraffic(Double appSubTypeTraffic) {
        this.appSubTypeTraffic = appSubTypeTraffic;
    }

    public Double getTotalTraffic() {
        return totalTraffic;
    }

    public void setTotalTraffic(Double totalTraffic) {
        this.totalTraffic = totalTraffic;
    }

    public Long getTotalUsers() {
        return totalUsers;
    }

    public void setTotalUsers(Long totalUsers) {
        this.totalUsers = totalUsers;
    }

    public String getAppStatu() {
        return appStatu;
    }

    public void setAppStatu(String appStatu) {
        this.appStatu = appStatu;
    }

    public Long getAppStatuCnt() {
        return appStatuCnt;
    }

    public void setAppStatuCnt(Long appStatuCnt) {
        this.appStatuCnt = appStatuCnt;
    }

    public Long getAppStatuAllCnt() {
        return appStatuAllCnt;
    }

    public void setAppStatuAllCnt(Long appStatuAllCnt) {
        this.appStatuAllCnt = appStatuAllCnt;
    }
}
