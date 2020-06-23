package com.broadtech.analyse.pojo.cmcc;

import java.util.List;

/**
 * @author jiangqingsong
 * @description 远程扫描采集原始数据
 * @date 2020-06-09 13:48
 */
public class AssetScanOrigin {
    private String ResourceName;
    private String TaskID;
    private String ScanTime;
    private String DeviceIPAddress;
    private String IPAddressOwnership;
    private String DeviceType;
    private String OSInfo;
    private String SystemFingerprintInfo;
    private List<OpenServiceOfPort> OpenServiceOfPort;
    private List<MessageOrientedMiddlewareScan> MessageOrientedMiddleware;
    private List<DataBaseInfoScan> dataBaseInfos;
    private String Runing;

    public String getRuning() {
        return Runing;
    }

    public void setRuning(String runing) {
        Runing = runing;
    }

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

    public String getDeviceIPAddress() {
        return DeviceIPAddress;
    }

    public void setDeviceIPAddress(String deviceIPAddress) {
        DeviceIPAddress = deviceIPAddress;
    }

    public String getIPAddressOwnership() {
        return IPAddressOwnership;
    }

    public void setIPAddressOwnership(String IPAddressOwnership) {
        this.IPAddressOwnership = IPAddressOwnership;
    }

    public String getDeviceType() {
        return DeviceType;
    }

    public void setDeviceType(String deviceType) {
        DeviceType = deviceType;
    }

    public String getOSInfo() {
        return OSInfo;
    }

    public void setOSInfo(String OSInfo) {
        this.OSInfo = OSInfo;
    }

    public String getSystemFingerprintInfo() {
        return SystemFingerprintInfo;
    }

    public void setSystemFingerprintInfo(String systemFingerprintInfo) {
        SystemFingerprintInfo = systemFingerprintInfo;
    }

    public List<com.broadtech.analyse.pojo.cmcc.OpenServiceOfPort> getOpenServiceOfPort() {
        return OpenServiceOfPort;
    }

    public void setOpenServiceOfPort(List<com.broadtech.analyse.pojo.cmcc.OpenServiceOfPort> openServiceOfPort) {
        OpenServiceOfPort = openServiceOfPort;
    }

    public List<MessageOrientedMiddlewareScan> getMessageOrientedMiddleware() {
        return MessageOrientedMiddleware;
    }

    public void setMessageOrientedMiddleware(List<MessageOrientedMiddlewareScan> messageOrientedMiddleware) {
        MessageOrientedMiddleware = messageOrientedMiddleware;
    }

    public List<DataBaseInfoScan> getDataBaseInfos() {
        return dataBaseInfos;
    }

    public void setDataBaseInfos(List<DataBaseInfoScan> dataBaseInfos) {
        this.dataBaseInfos = dataBaseInfos;
    }
}
