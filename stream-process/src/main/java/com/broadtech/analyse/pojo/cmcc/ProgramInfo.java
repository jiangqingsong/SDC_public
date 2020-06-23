package com.broadtech.analyse.pojo.cmcc;

import java.util.List;

/**
 * @author jiangqingsong
 * @description 进程信息
 * @date 2020-06-09 16:33
 */
public class ProgramInfo {
    private String Pid;
    private String Name;
    private String Version;
    private String User;
    private String Group;
    private String Path;
    private String Parameter;
    private String StartTime;
    private List<Integer> Port;

    public List<Integer> getPort() {
        return Port;
    }

    public void setPort(List<Integer> port) {
        Port = port;
    }

    public String getStartTime() {
        return StartTime;
    }

    public void setStartTime(String startTime) {
        StartTime = startTime;
    }

    public String getPid() {
        return Pid;
    }

    public void setPid(String pid) {
        this.Pid = pid;
    }

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

    public String getUser() {
        return User;
    }

    public void setUser(String user) {
        this.User = user;
    }

    public String getGroup() {
        return Group;
    }

    public void setGroup(String group) {
        this.Group = group;
    }

    public String getPath() {
        return Path;
    }

    public void setPath(String path) {
        this.Path = path;
    }

    public String getParameter() {
        return Parameter;
    }

    public void setParameter(String parameter) {
        this.Parameter = parameter;
    }
}
