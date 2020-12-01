package com.broadtech.analyse.pojo.cmcc.find;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

/**
 * @author leo.J
 * @description
 * @date 2020-06-16 10:57
 */
public class RetKafkaMessage {
    private String TaskID;
    private String IPAddress;

    public RetKafkaMessage(String TaskID, String IPAddress) {
        this.TaskID = TaskID;
        this.IPAddress = IPAddress;
    }

    public String getTaskID() {
        return TaskID;
    }

    public void setTaskID(String taskID) {
        TaskID = taskID;
    }

    public String getIPAddress() {
        return IPAddress;
    }

    public void setIPAddress(String IPAddress) {
        this.IPAddress = IPAddress;
    }

    public static void main(String[] args) {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("A", "A1");
        jsonObject.put("B", "B1");
        System.out.println(jsonObject.toJSONString());
    }
}
