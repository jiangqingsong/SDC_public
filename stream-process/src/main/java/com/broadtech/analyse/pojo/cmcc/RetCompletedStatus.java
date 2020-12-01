package com.broadtech.analyse.pojo.cmcc;

/**
 * @author leo.J
 * @description 插入完成返回状态
 * @date 2020-06-13 14:40
 */
public class RetCompletedStatus {
    private String taskId;
    private Integer status;

    public RetCompletedStatus(String taskId, Integer status) {
        this.taskId = taskId;
        this.status = status;
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }
}
