package com.broadtech.analyse.pojo.cmcc.find;

import java.util.List;

/**
 * @author leo.J
 * @description login 资产发现
 * @date 2020-06-12 19:15
 */
public class AssetFindLogin {
    private String ResourceName;
    private String TaskID;
    private String ScanTime;
    //private String ResourceInfo;
    private String SwitchIP;
    private String Level;
    private List<ARP> Arp;
    private List<Neighbor> Neighbor;
    //private List<Route> Route;
    private List<Ipv4Route> Ipv4Route;
    private List<Ipv6Route> Ipv6Route;


    public String getResourceName() {
        return ResourceName;
    }

    public void setResourceName(String resourceName) {
        ResourceName = resourceName;
    }

    public String getTaskID() {
        return TaskID;
    }

    public void setTaskID(String taskID) {
        TaskID = taskID;
    }

    public String getScanTime() {
        return ScanTime;
    }

    public void setScanTime(String scanTime) {
        ScanTime = scanTime;
    }

    public String getSwitchIP() {
        return SwitchIP;
    }

    public void setSwitchIP(String switchIP) {
        SwitchIP = switchIP;
    }

    public String getLevel() {
        return Level;
    }

    public void setLevel(String level) {
        Level = level;
    }

    public List<ARP> getArp() {
        return Arp;
    }

    public void setArp(List<ARP> arp) {
        Arp = arp;
    }

    public List<Neighbor> getNeighbor() {
        return Neighbor;
    }

    public void setNeighbor(List<Neighbor> neighbor) {
        Neighbor = neighbor;
    }

    public List<com.broadtech.analyse.pojo.cmcc.find.Ipv4Route> getIpv4Route() {
        return Ipv4Route;
    }

    public void setIpv4Route(List<com.broadtech.analyse.pojo.cmcc.find.Ipv4Route> ipv4Route) {
        Ipv4Route = ipv4Route;
    }

    public List<com.broadtech.analyse.pojo.cmcc.find.Ipv6Route> getIpv6Route() {
        return Ipv6Route;
    }

    public void setIpv6Route(List<com.broadtech.analyse.pojo.cmcc.find.Ipv6Route> ipv6Route) {
        Ipv6Route = ipv6Route;
    }
}
