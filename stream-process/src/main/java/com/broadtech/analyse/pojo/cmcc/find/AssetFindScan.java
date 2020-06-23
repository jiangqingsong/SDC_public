package com.broadtech.analyse.pojo.cmcc.find;

import java.util.List;

/**
 * @author jiangqingsong
 * @description 远程扫描发现资产
 * @date 2020-06-12 21:08
 */
public class AssetFindScan {
    private String ResourceName;
    private String TaskID;
    private String ScanTime;
    private List<String> ips;

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

    public List<String> getIps() {
        return ips;
    }

    public void setIps(List<String> ips) {
        this.ips = ips;
    }
}
