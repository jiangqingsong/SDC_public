package com.broadtech.analyse.pojo.abnormal;

/**
 * @author leo.J
 * @description
 * @date 2020-08-21 11:08
 */
public class Dpi {
    public String SrcIPAddress;
    public String SrcPort;
    public String DestIPAddress;
    public String DestPort;
    public String Protocol;
    public String StartTime;
    public String EndTime;
    public String ConnectDuration;
    public String PacketSize;
    public String PacketNumber;
    public String UpstreamTraffic;
    public String DownstreamTraffic;
    public String TrafficSize;
    public String DomainName;
    public String Url;
    public String FileName;
    public String id;

    public Dpi(String srcIPAddress, String srcPort, String destIPAddress, String destPort, String protocol, String startTime, String endTime, String connectDuration, String packetSize, String packetNumber, String upstreamTraffic, String downstreamTraffic, String trafficSize, String domainName, String url, String fileName, String id) {
        SrcIPAddress = srcIPAddress;
        SrcPort = srcPort;
        DestIPAddress = destIPAddress;
        DestPort = destPort;
        Protocol = protocol;
        StartTime = startTime;
        EndTime = endTime;
        ConnectDuration = connectDuration;
        PacketSize = packetSize;
        PacketNumber = packetNumber;
        UpstreamTraffic = upstreamTraffic;
        DownstreamTraffic = downstreamTraffic;
        TrafficSize = trafficSize;
        DomainName = domainName;
        Url = url;
        FileName = fileName;
        this.id = id;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getSrcIPAddress() {
        return SrcIPAddress;
    }

    public void setSrcIPAddress(String srcIPAddress) {
        SrcIPAddress = srcIPAddress;
    }

    public String getSrcPort() {
        return SrcPort;
    }

    public void setSrcPort(String srcPort) {
        SrcPort = srcPort;
    }

    public String getDestIPAddress() {
        return DestIPAddress;
    }

    public void setDestIPAddress(String destIPAddress) {
        DestIPAddress = destIPAddress;
    }

    public String getDestPort() {
        return DestPort;
    }

    public void setDestPort(String destPort) {
        DestPort = destPort;
    }

    public String getProtocol() {
        return Protocol;
    }

    public void setProtocol(String protocol) {
        Protocol = protocol;
    }

    public String getStartTime() {
        return StartTime;
    }

    public void setStartTime(String startTime) {
        StartTime = startTime;
    }

    public String getEndTime() {
        return EndTime;
    }

    public void setEndTime(String endTime) {
        EndTime = endTime;
    }

    public String getConnectDuration() {
        return ConnectDuration;
    }

    public void setConnectDuration(String connectDuration) {
        ConnectDuration = connectDuration;
    }

    public String getPacketSize() {
        return PacketSize;
    }

    public void setPacketSize(String packetSize) {
        PacketSize = packetSize;
    }

    public String getPacketNumber() {
        return PacketNumber;
    }

    public void setPacketNumber(String packetNumber) {
        PacketNumber = packetNumber;
    }

    public String getUpstreamTraffic() {
        return UpstreamTraffic;
    }

    public void setUpstreamTraffic(String upstreamTraffic) {
        UpstreamTraffic = upstreamTraffic;
    }

    public String getDownstreamTraffic() {
        return DownstreamTraffic;
    }

    public void setDownstreamTraffic(String downstreamTraffic) {
        DownstreamTraffic = downstreamTraffic;
    }

    public String getTrafficSize() {
        return TrafficSize;
    }

    public void setTrafficSize(String trafficSize) {
        TrafficSize = trafficSize;
    }

    public String getDomainName() {
        return DomainName;
    }

    public void setDomainName(String domainName) {
        DomainName = domainName;
    }

    public String getUrl() {
        return Url;
    }

    public void setUrl(String url) {
        Url = url;
    }

    public String getFileName() {
        return FileName;
    }

    public void setFileName(String fileName) {
        FileName = fileName;
    }

    @Override
    public String toString() {
        return "Dpi{" +
                "SrcIPAddress='" + SrcIPAddress + '\'' +
                ", SrcPort='" + SrcPort + '\'' +
                ", DestIPAddress='" + DestIPAddress + '\'' +
                ", DestPort='" + DestPort + '\'' +
                ", Protocol='" + Protocol + '\'' +
                ", StartTime='" + StartTime + '\'' +
                ", EndTime='" + EndTime + '\'' +
                ", ConnectDuration='" + ConnectDuration + '\'' +
                ", PacketSize='" + PacketSize + '\'' +
                ", PacketNumber='" + PacketNumber + '\'' +
                ", UpstreamTraffic='" + UpstreamTraffic + '\'' +
                ", DownstreamTraffic='" + DownstreamTraffic + '\'' +
                ", TrafficSize='" + TrafficSize + '\'' +
                ", DomainName='" + DomainName + '\'' +
                ", Url='" + Url + '\'' +
                ", FileName='" + FileName + '\'' +
                ", id='" + id + '\'' +
                '}';
    }
}
