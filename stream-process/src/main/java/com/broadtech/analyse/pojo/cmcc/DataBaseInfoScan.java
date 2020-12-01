package com.broadtech.analyse.pojo.cmcc;

/**
 * @author leo.J
 * @description 主动扫描版
 * @date 2020-06-12 14:22
 */
public class DataBaseInfoScan {
    private String Name;
    private String Version;
    private String Port;

    public String getName() {
        return Name;
    }

    public void setName(String name) {
        Name = name;
    }

    public String getVersion() {
        return Version;
    }

    public void setVersion(String version) {
        Version = version;
    }

    public String getPort() {
        return Port;
    }

    public void setPort(String port) {
        Port = port;
    }
}
