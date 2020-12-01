package com.broadtech.analyse.pojo.main;

/**
 * @author leo.J
 * @description 统一后的告警事件实体类
 * @date 2020-09-18 14:13
 */
public class AlarmEvenUnify {
    public String eventName;
    public String firstEventType;
    public String secondEventType;
    public String thirdEventType;
    public String eventGrade;
    //public String discoveryTime;
    public String alarmTimes;
    public String deviceIpAddress;
    public String deviceType;
    public String deviceFactory;
    public String deviceModel;
    public String deviceName;
    public String srcIpAddress;
    public String srcPort;
    public String srcMacAddress;
    public String destIpAddress;
    public String destPort;
    //public String affectDevice;
    public String eventDesc;
    /*public String attackerSourceType;
    public String attackerSource;*/
    public String threatType;
    public String threatGeo;
    public String threatScore;
    public String windowStartTime;
    public String windowEndTime;
    public String trafficSize;
    public String fileName;
    public String keyword;

    public AlarmEvenUnify() {
    }

    public AlarmEvenUnify(String eventName, String firstEventType, String secondEventType, String thirdEventType, String eventGrade, String alarmTimes, String deviceIpAddress, String deviceType, String deviceFactory, String deviceModel, String deviceName, String srcIpAddress, String srcPort, String srcMacAddress, String destIpAddress, String destPort, String eventDesc, String threatType, String threatGeo, String threatScore, String windowStartTime, String windowEndTime, String trafficSize, String fileName, String keyword) {
        this.eventName = eventName;
        this.firstEventType = firstEventType;
        this.secondEventType = secondEventType;
        this.thirdEventType = thirdEventType;
        this.eventGrade = eventGrade;
        this.alarmTimes = alarmTimes;
        this.deviceIpAddress = deviceIpAddress;
        this.deviceType = deviceType;
        this.deviceFactory = deviceFactory;
        this.deviceModel = deviceModel;
        this.deviceName = deviceName;
        this.srcIpAddress = srcIpAddress;
        this.srcPort = srcPort;
        this.srcMacAddress = srcMacAddress;
        this.destIpAddress = destIpAddress;
        this.destPort = destPort;
        this.eventDesc = eventDesc;
        this.threatType = threatType;
        this.threatGeo = threatGeo;
        this.threatScore = threatScore;
        this.windowStartTime = windowStartTime;
        this.windowEndTime = windowEndTime;
        this.trafficSize = trafficSize;
        this.fileName = fileName;
        this.keyword = keyword;
    }

    public String getEventName() {
        return eventName;
    }

    public void setEventName(String eventName) {
        this.eventName = eventName;
    }

    public String getFirstEventType() {
        return firstEventType;
    }

    public void setFirstEventType(String firstEventType) {
        this.firstEventType = firstEventType;
    }

    public String getSecondEventType() {
        return secondEventType;
    }

    public void setSecondEventType(String secondEventType) {
        this.secondEventType = secondEventType;
    }

    public String getThirdEventType() {
        return thirdEventType;
    }

    public void setThirdEventType(String thirdEventType) {
        this.thirdEventType = thirdEventType;
    }

    public String getEventGrade() {
        return eventGrade;
    }

    public void setEventGrade(String eventGrade) {
        this.eventGrade = eventGrade;
    }

    /*public String getDiscoveryTime() {
        return discoveryTime;
    }

    public void setDiscoveryTime(String discoveryTime) {
        this.discoveryTime = discoveryTime;
    }*/

    public String getAlarmTimes() {
        return alarmTimes;
    }

    public void setAlarmTimes(String alarmTimes) {
        this.alarmTimes = alarmTimes;
    }

    public String getDeviceIpAddress() {
        return deviceIpAddress;
    }

    public void setDeviceIpAddress(String deviceIpAddress) {
        this.deviceIpAddress = deviceIpAddress;
    }

    public String getDeviceType() {
        return deviceType;
    }

    public void setDeviceType(String deviceType) {
        this.deviceType = deviceType;
    }

    public String getDeviceFactory() {
        return deviceFactory;
    }

    public void setDeviceFactory(String deviceFactory) {
        this.deviceFactory = deviceFactory;
    }

    public String getDeviceModel() {
        return deviceModel;
    }

    public void setDeviceModel(String deviceModel) {
        this.deviceModel = deviceModel;
    }

    public String getDeviceName() {
        return deviceName;
    }

    public void setDeviceName(String deviceName) {
        this.deviceName = deviceName;
    }

    public String getSrcIpAddress() {
        return srcIpAddress;
    }

    public void setSrcIpAddress(String srcIpAddress) {
        this.srcIpAddress = srcIpAddress;
    }

    public String getSrcPort() {
        return srcPort;
    }

    public void setSrcPort(String srcPort) {
        this.srcPort = srcPort;
    }

    public String getSrcMacAddress() {
        return srcMacAddress;
    }

    public void setSrcMacAddress(String srcMacAddress) {
        this.srcMacAddress = srcMacAddress;
    }

    public String getDestIpAddress() {
        return destIpAddress;
    }

    public void setDestIpAddress(String destIpAddress) {
        this.destIpAddress = destIpAddress;
    }

    public String getDestPort() {
        return destPort;
    }

    public void setDestPort(String destPort) {
        this.destPort = destPort;
    }

    /*public String getAffectDevice() {
        return affectDevice;
    }

    public void setAffectDevice(String affectDevice) {
        this.affectDevice = affectDevice;
    }*/

    public String getEventDesc() {
        return eventDesc;
    }

    public void setEventDesc(String eventDesc) {
        this.eventDesc = eventDesc;
    }

    /*public String getAttackerSourceType() {
        return attackerSourceType;
    }

    public void setAttackerSourceType(String attackerSourceType) {
        this.attackerSourceType = attackerSourceType;
    }

    public String getAttackerSource() {
        return attackerSource;
    }

    public void setAttackerSource(String attackerSource) {
        this.attackerSource = attackerSource;
    }*/

    public String getThreatType() {
        return threatType;
    }

    public void setThreatType(String threatType) {
        this.threatType = threatType;
    }

    public String getThreatGeo() {
        return threatGeo;
    }

    public void setThreatGeo(String threatGeo) {
        this.threatGeo = threatGeo;
    }

    public String getThreatScore() {
        return threatScore;
    }

    public void setThreatScore(String threatScore) {
        this.threatScore = threatScore;
    }

    public String getWindowStartTime() {
        return windowStartTime;
    }

    public void setWindowStartTime(String windowStartTime) {
        this.windowStartTime = windowStartTime;
    }

    public String getWindowEndTime() {
        return windowEndTime;
    }

    public void setWindowEndTime(String windowEndTime) {
        this.windowEndTime = windowEndTime;
    }

    public String getTrafficSize() {
        return trafficSize;
    }

    public void setTrafficSize(String trafficSize) {
        this.trafficSize = trafficSize;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getKeyword() {
        return keyword;
    }

    public void setKeyword(String keyword) {
        this.keyword = keyword;
    }
}
