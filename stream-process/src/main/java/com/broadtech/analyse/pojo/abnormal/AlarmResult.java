package com.broadtech.analyse.pojo.abnormal;

/**
 * @author leo.J
 * @description 告警表
 * @date 2020-08-21 18:02
 */
public class AlarmResult {
    public String srcIpAddress;
    public String destIpAddress;
    public String windowStart;
    public String windowEnd;
    public String trafficSize;
    public String fileName;
    public String keyword;
    public String geo;
    public String category;
    public String score;
    public Integer alarmType;
    public String traceIds;

    public AlarmResult(String srcIpAddress, String destIpAddress, String windowStart, String windowEnd, String trafficSize, String fileName, String keyword, String geo, String category, String score, Integer alarmType, String traceIds) {
        this.srcIpAddress = srcIpAddress;
        this.destIpAddress = destIpAddress;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.trafficSize = trafficSize;
        this.fileName = fileName;
        this.keyword = keyword;
        this.geo = geo;
        this.category = category;
        this.score = score;
        this.alarmType = alarmType;
        this.traceIds = traceIds;
    }

    public String getTraceIds() {
        return traceIds;
    }

    public void setTraceIds(String traceIds) {
        this.traceIds = traceIds;
    }

    public String getWindowStart() {
        return windowStart;
    }

    public void setWindowStart(String windowStart) {
        this.windowStart = windowStart;
    }

    public String getGeo() {
        return geo;
    }

    public void setGeo(String geo) {
        this.geo = geo;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getScore() {
        return score;
    }

    public void setScore(String score) {
        this.score = score;
    }

    public String getSrcIpAddress() {
        return srcIpAddress;
    }

    public void setSrcIpAddress(String srcIpAddress) {
        this.srcIpAddress = srcIpAddress;
    }

    public String getDestIpAddress() {
        return destIpAddress;
    }

    public void setDestIpAddress(String destIpAddress) {
        this.destIpAddress = destIpAddress;
    }

    public String getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(String windowEnd) {
        this.windowEnd = windowEnd;
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

    public Integer getAlarmType() {
        return alarmType;
    }

    public void setAlarmType(Integer alarmType) {
        this.alarmType = alarmType;
    }

}
