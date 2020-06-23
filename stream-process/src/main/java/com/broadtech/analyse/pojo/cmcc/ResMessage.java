package com.broadtech.analyse.pojo.cmcc;

/**
 * @author jiangqingsong
 * @description 返回消息体
 * @date 2020-06-10 19:43
 */
public class ResMessage {
    private int state;//1: success 0：fail
    private String message;

    public ResMessage(int state, String message) {
        this.state = state;
        this.message = message;
    }

    public int getState() {
        return state;
    }

    public void setState(int state) {
        this.state = state;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
