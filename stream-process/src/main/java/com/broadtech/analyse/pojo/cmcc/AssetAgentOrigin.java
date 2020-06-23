package com.broadtech.analyse.pojo.cmcc;

import java.util.List;

/**
 * @author jiangqingsong
 * @description Agent代理采集
 * @date 2020-06-09 13:48
 */
public class AssetAgentOrigin {
    private String ResourceName;
    private String TaskID;
    private String AssetID;
    private String ScanTime;
    private String DeviceName;
    private String DeviceType;
    private List<String> deviceIpAddresss;
    private String OSInfo;
    private List<String> patchProperties;
    private String KernelVersion;
    private List<Integer> openServiceOfPorts;
    private List<ProgramInfo> programInfos;
    private List<MessageOrientedMiddleware> messageOrientedMiddlewares;
    private List<DataBaseInfo> dataBaseInfos;

    public String getResourceName() {
        return ResourceName;
    }

    public void setResourceName(String resourceName) {
        this.ResourceName = resourceName;
    }

    public String getTaskID() {
        return TaskID;
    }

    public void setTaskID(String taskID) {
        this.TaskID = taskID;
    }

    public String getAssetID() {
        return AssetID;
    }

    public void setAssetID(String assetID) {
        this.AssetID = assetID;
    }

    public String getScanTime() {
        return ScanTime;
    }

    public void setScanTime(String scanTime) {
        this.ScanTime = scanTime;
    }

    public String getDeviceName() {
        return DeviceName;
    }

    public void setDeviceName(String deviceName) {
        this.DeviceName = deviceName;
    }

    public String getDeviceType() {
        return DeviceType;
    }

    public void setDeviceType(String deviceType) {
        this.DeviceType = deviceType;
    }

    public List<String> getDeviceIpAddresss() {
        return deviceIpAddresss;
    }

    public void setDeviceIpAddresss(List<String> deviceIpAddresss) {
        this.deviceIpAddresss = deviceIpAddresss;
    }

    public String getOSInfo() {
        return OSInfo;
    }

    public void setOSInfo(String OSInfo) {
        this.OSInfo = OSInfo;
    }

    public List<String> getPatchProperties() {
        return patchProperties;
    }

    public void setPatchProperties(List<String> patchProperties) {
        this.patchProperties = patchProperties;
    }

    public String getKernelVersion() {
        return KernelVersion;
    }

    public void setKernelVersion(String kernelVersion) {
        this.KernelVersion = kernelVersion;
    }

    public List<Integer> getOpenServiceOfPorts() {
        return openServiceOfPorts;
    }

    public void setOpenServiceOfPorts(List<Integer> openServiceOfPorts) {
        this.openServiceOfPorts = openServiceOfPorts;
    }

    public List<ProgramInfo> getProgramInfos() {
        return programInfos;
    }

    public void setProgramInfos(List<ProgramInfo> programInfos) {
        this.programInfos = programInfos;
    }

    public List<MessageOrientedMiddleware> getMessageOrientedMiddlewares() {
        return messageOrientedMiddlewares;
    }

    public void setMessageOrientedMiddlewares(List<MessageOrientedMiddleware> messageOrientedMiddlewares) {
        this.messageOrientedMiddlewares = messageOrientedMiddlewares;
    }

    public List<DataBaseInfo> getDataBaseInfos() {
        return dataBaseInfos;
    }

    public void setDataBaseInfos(List<DataBaseInfo> dataBaseInfos) {
        this.dataBaseInfos = dataBaseInfos;
    }
}
