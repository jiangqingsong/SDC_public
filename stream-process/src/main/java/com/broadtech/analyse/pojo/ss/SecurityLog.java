package com.broadtech.analyse.pojo.ss;

/**
 * @author jiangqingsong
 * @description 安全日志POJO
 * @date 2020-08-04 11:46
 */
public class SecurityLog {
    private String id;
    private String eventrecvtime;
    private String eventgeneratetime;
    private String eventdurationtime;
    private String username;
    private String srcipaddress;
    private String srcmacaddress;
    private String srcport;
    private String operation;
    private String destipaddress;
    private String destmacaddress;
    private String destport;
    private String eventname;
    private String firsteventtype;
    private String secondeventtype;
    private String thirdeventtype;
    private String eventdesc;
    private String eventgrade;
    private String networkprotocal;
    private String netappprotocal;
    private String deviceipaddress;
    private String devicename;
    private String devicetype;
    private String devicefactory;
    private String devicemodel;
    private String pid;
    private String url;
    private String domain;
    private String mail;
    private String malwaresample;
    private String softwarename;
    private String softwareversion;

    public SecurityLog(String id, String eventrecvtime, String eventgeneratetime, String eventdurationtime, String username, String srcipaddress, String srcmacaddress, String srcport, String operation, String destipaddress, String destmacaddress, String destport, String eventname, String firsteventtype, String secondeventtype, String thirdeventtype, String eventdesc, String eventgrade, String networkprotocal, String netappprotocal, String deviceipaddress, String devicename, String devicetype, String devicefactory, String devicemodel, String pid, String url, String domain, String mail, String malwaresample, String softwarename, String softwareversion) {
        this.id = id;
        this.eventrecvtime = eventrecvtime;
        this.eventgeneratetime = eventgeneratetime;
        this.eventdurationtime = eventdurationtime;
        this.username = username;
        this.srcipaddress = srcipaddress;
        this.srcmacaddress = srcmacaddress;
        this.srcport = srcport;
        this.operation = operation;
        this.destipaddress = destipaddress;
        this.destmacaddress = destmacaddress;
        this.destport = destport;
        this.eventname = eventname;
        this.firsteventtype = firsteventtype;
        this.secondeventtype = secondeventtype;
        this.thirdeventtype = thirdeventtype;
        this.eventdesc = eventdesc;
        this.eventgrade = eventgrade;
        this.networkprotocal = networkprotocal;
        this.netappprotocal = netappprotocal;
        this.deviceipaddress = deviceipaddress;
        this.devicename = devicename;
        this.devicetype = devicetype;
        this.devicefactory = devicefactory;
        this.devicemodel = devicemodel;
        this.pid = pid;
        this.url = url;
        this.domain = domain;
        this.mail = mail;
        this.malwaresample = malwaresample;
        this.softwarename = softwarename;
        this.softwareversion = softwareversion;
    }

    public String getSoftwarename() {
        return softwarename;
    }

    public void setSoftwarename(String softwarename) {
        this.softwarename = softwarename;
    }

    public String getSoftwareversion() {
        return softwareversion;
    }

    public void setSoftwareversion(String softwareversion) {
        this.softwareversion = softwareversion;
    }

    public SecurityLog() {
    }


    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getEventrecvtime() {
        return eventrecvtime;
    }

    public void setEventrecvtime(String eventrecvtime) {
        this.eventrecvtime = eventrecvtime;
    }

    public String getEventgeneratetime() {
        return eventgeneratetime;
    }

    public void setEventgeneratetime(String eventgeneratetime) {
        this.eventgeneratetime = eventgeneratetime;
    }

    public String getEventdurationtime() {
        return eventdurationtime;
    }

    public void setEventdurationtime(String eventdurationtime) {
        this.eventdurationtime = eventdurationtime;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getSrcipaddress() {
        return srcipaddress;
    }

    public void setSrcipaddress(String srcipaddress) {
        this.srcipaddress = srcipaddress;
    }

    public String getSrcmacaddress() {
        return srcmacaddress;
    }

    public void setSrcmacaddress(String srcmacaddress) {
        this.srcmacaddress = srcmacaddress;
    }

    public String getSrcport() {
        return srcport;
    }

    public void setSrcport(String srcport) {
        this.srcport = srcport;
    }

    public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }

    public String getDestipaddress() {
        return destipaddress;
    }

    public void setDestipaddress(String destipaddress) {
        this.destipaddress = destipaddress;
    }

    public String getDestmacaddress() {
        return destmacaddress;
    }

    public void setDestmacaddress(String destmacaddress) {
        this.destmacaddress = destmacaddress;
    }

    public String getDestport() {
        return destport;
    }

    public void setDestport(String destport) {
        this.destport = destport;
    }

    public String getEventname() {
        return eventname;
    }

    public void setEventname(String eventname) {
        this.eventname = eventname;
    }

    public String getFirsteventtype() {
        return firsteventtype;
    }

    public void setFirsteventtype(String firsteventtype) {
        this.firsteventtype = firsteventtype;
    }

    public String getSecondeventtype() {
        return secondeventtype;
    }

    public void setSecondeventtype(String secondeventtype) {
        this.secondeventtype = secondeventtype;
    }

    public String getThirdeventtype() {
        return thirdeventtype;
    }

    public void setThirdeventtype(String thirdeventtype) {
        this.thirdeventtype = thirdeventtype;
    }

    public String getEventdesc() {
        return eventdesc;
    }

    public void setEventdesc(String eventdesc) {
        this.eventdesc = eventdesc;
    }

    public String getEventgrade() {
        return eventgrade;
    }

    public void setEventgrade(String eventgrade) {
        this.eventgrade = eventgrade;
    }

    public String getNetworkprotocal() {
        return networkprotocal;
    }

    public void setNetworkprotocal(String networkprotocal) {
        this.networkprotocal = networkprotocal;
    }

    public String getNetappprotocal() {
        return netappprotocal;
    }

    public void setNetappprotocal(String netappprotocal) {
        this.netappprotocal = netappprotocal;
    }

    public String getDeviceipaddress() {
        return deviceipaddress;
    }

    public void setDeviceipaddress(String deviceipaddress) {
        this.deviceipaddress = deviceipaddress;
    }

    public String getDevicename() {
        return devicename;
    }

    public void setDevicename(String devicename) {
        this.devicename = devicename;
    }

    public String getDevicetype() {
        return devicetype;
    }

    public void setDevicetype(String devicetype) {
        this.devicetype = devicetype;
    }

    public String getDevicefactory() {
        return devicefactory;
    }

    public void setDevicefactory(String devicefactory) {
        this.devicefactory = devicefactory;
    }

    public String getDevicemodel() {
        return devicemodel;
    }

    public void setDevicemodel(String devicemodel) {
        this.devicemodel = devicemodel;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public String getMail() {
        return mail;
    }

    public void setMail(String mail) {
        this.mail = mail;
    }

    public String getMalwaresample() {
        return malwaresample;
    }

    public void setMalwaresample(String malwaresample) {
        this.malwaresample = malwaresample;
    }

}
