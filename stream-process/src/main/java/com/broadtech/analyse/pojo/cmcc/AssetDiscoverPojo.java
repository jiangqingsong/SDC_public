package com.broadtech.analyse.pojo.cmcc;

import java.util.ArrayList;

/**
 * @author jiangqingsong
 * @description 资产发现 json解析对应Pojo
 * @date 2020-06-04 14:24
 */
public class AssetDiscoverPojo {

    private String resource_name;
    private String task_id;
    private String scan_time;
    private ArrayList<String> resource_info;

    public String getResource_name() {
        return resource_name;
    }

    public void setResource_name(String resource_name) {
        this.resource_name = resource_name;
    }

    public String getTask_id() {
        return task_id;
    }

    public void setTask_id(String task_id) {
        this.task_id = task_id;
    }

    public String getScan_time() {
        return scan_time;
    }

    public void setScan_time(String scan_time) {
        this.scan_time = scan_time;
    }

    public ArrayList<String> getResource_info() {
        return resource_info;
    }

    public void setResource_info(ArrayList<String> resource_info) {
        this.resource_info = resource_info;
    }
}
