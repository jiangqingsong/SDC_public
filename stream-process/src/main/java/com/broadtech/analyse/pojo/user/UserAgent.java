package com.broadtech.analyse.pojo.user;

/**
 * @author 吴榜元
 * @create 2020-05-11 14:18
 **/
public class UserAgent {
    /**
     * 设备型号
     */
    private String deviceModel;
    /**
     * 系统名称
     */
    private String os;
    /**
     * 系统版本号
     */
    private String osVer;
    /**
     * 浏览器名称
     */
    private String browser;
    /**
     * 浏览器版本号
     */
    private String browserVer;


    public UserAgent(String deviceModel, String os, String osVer, String browser, String browserVer) {
        this.deviceModel = deviceModel;
        this.os = os;
        this.osVer = osVer;
        this.browser = browser;
        this.browserVer = browserVer;
    }

    @Override
    public String toString() {
        return "UserAgent{" +
                "deviceModel='" + deviceModel + '\'' +
                ", os='" + os + '\'' +
                ", osVer='" + osVer + '\'' +
                ", browser='" + browser + '\'' +
                ", browserVer='" + browserVer + '\'' +
                '}';
    }

}
