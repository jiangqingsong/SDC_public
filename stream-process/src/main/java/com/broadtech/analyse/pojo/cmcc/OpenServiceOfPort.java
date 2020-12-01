package com.broadtech.analyse.pojo.cmcc;

/**
 * @author leo.J
 * @description 远程扫描版本
 * @date 2020-06-06 14:00
 */
public class OpenServiceOfPort {
    private String Name;
    private String Port;
    private String Version;

    public String getName() {
        return Name;
    }

    public void setName(String name) {
        this.Name = name;
    }

    public String getPort() {
        return Port;
    }

    public void setPort(String port) {
        this.Port = port;
    }

    public String getVersion() {
        return Version;
    }

    public void setVersion(String version) {
        this.Version = version;
    }
}
