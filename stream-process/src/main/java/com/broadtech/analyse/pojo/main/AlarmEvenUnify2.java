package com.broadtech.analyse.pojo.main;

/**
 * @author leo.J
 * @description
 * @date 2020-09-20 18:16
 */
public class AlarmEvenUnify2 {
    private String event_name;
    private String second_event_type;
    private String danger_level;
    private String discovery_time;
    private String alarm_times;
    private String device_ip_address;
    private String device_type;
    private String src_ip_address;
    private String src_port;
    private String src_ip_local;
    private String dest_ip_address;
    private String dest_port;
    private String affect_device;
    private String event_desc;
    private String trace_log_ids;

    public AlarmEvenUnify2(String event_name, String second_event_type, String danger_level, String discovery_time, String alarm_times, String device_ip_address, String device_type, String src_ip_address, String src_port, String src_ip_local, String dest_ip_address, String dest_port, String affect_device, String event_desc, String trace_log_ids) {
        this.event_name = event_name;
        this.second_event_type = second_event_type;
        this.danger_level = danger_level;
        this.discovery_time = discovery_time;
        this.alarm_times = alarm_times;
        this.device_ip_address = device_ip_address;
        this.device_type = device_type;
        this.src_ip_address = src_ip_address;
        this.src_port = src_port;
        this.src_ip_local = src_ip_local;
        this.dest_ip_address = dest_ip_address;
        this.dest_port = dest_port;
        this.affect_device = affect_device;
        this.event_desc = event_desc;
        this.trace_log_ids = trace_log_ids;
    }

    public String getEvent_name() {
        return event_name;
    }

    public void setEvent_name(String event_name) {
        this.event_name = event_name;
    }

    public String getSecond_event_type() {
        return second_event_type;
    }

    public void setSecond_event_type(String second_event_type) {
        this.second_event_type = second_event_type;
    }

    public String getDanger_level() {
        return danger_level;
    }

    public void setDanger_level(String danger_level) {
        this.danger_level = danger_level;
    }

    public String getDiscovery_time() {
        return discovery_time;
    }

    public void setDiscovery_time(String discovery_time) {
        this.discovery_time = discovery_time;
    }

    public String getAlarm_times() {
        return alarm_times;
    }

    public void setAlarm_times(String alarm_times) {
        this.alarm_times = alarm_times;
    }

    public String getDevice_ip_address() {
        return device_ip_address;
    }

    public void setDevice_ip_address(String device_ip_address) {
        this.device_ip_address = device_ip_address;
    }

    public String getDevice_type() {
        return device_type;
    }

    public void setDevice_type(String device_type) {
        this.device_type = device_type;
    }

    public String getSrc_ip_address() {
        return src_ip_address;
    }

    public void setSrc_ip_address(String src_ip_address) {
        this.src_ip_address = src_ip_address;
    }

    public String getSrc_port() {
        return src_port;
    }

    public void setSrc_port(String src_port) {
        this.src_port = src_port;
    }

    public String getSrc_ip_local() {
        return src_ip_local;
    }

    public void setSrc_ip_local(String src_ip_local) {
        this.src_ip_local = src_ip_local;
    }

    public String getDest_ip_address() {
        return dest_ip_address;
    }

    public void setDest_ip_address(String dest_ip_address) {
        this.dest_ip_address = dest_ip_address;
    }

    public String getDest_port() {
        return dest_port;
    }

    public void setDest_port(String dest_port) {
        this.dest_port = dest_port;
    }

    public String getAffect_device() {
        return affect_device;
    }

    public void setAffect_device(String affect_device) {
        this.affect_device = affect_device;
    }

    public String getEvent_desc() {
        return event_desc;
    }

    public void setEvent_desc(String event_desc) {
        this.event_desc = event_desc;
    }

    public String getTrace_log_ids() {
        return trace_log_ids;
    }

    public void setTrace_log_ids(String trace_log_ids) {
        this.trace_log_ids = trace_log_ids;
    }
}
