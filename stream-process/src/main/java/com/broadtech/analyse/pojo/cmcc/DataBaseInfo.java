package com.broadtech.analyse.pojo.cmcc;

/**
 * @author leo.J
 * @description 数据库信息
 * @date 2020-06-09 16:36
 */
public class DataBaseInfo {
    private String Name;
    private String Version;
    private String Port;
    private String User;

    public String getName() {
        return Name;
    }

    public void setName(String name) {
        this.Name = name;
    }

    public String getVersion() {
        return Version;
    }

    public void setVersion(String version) {
        this.Version = version;
    }

    public String getPort() {
        return Port;
    }

    public void setPort(String port) {
        this.Port = port;
    }

    public String getUser() {
        return User;
    }

    public void setUser(String user) {
        this.User = user;
    }

}
