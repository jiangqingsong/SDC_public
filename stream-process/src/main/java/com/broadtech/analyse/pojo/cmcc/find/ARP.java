package com.broadtech.analyse.pojo.cmcc.find;

/**
 * @author leo.J
 * @description ARP表
 * @date 2020-06-12 19:18
 */
public class ARP {
    private String IP;
    private String Mac;
    private String Interface;

    public String getIP() {
        return IP;
    }

    public void setIP(String IP) {
        this.IP = IP;
    }

    public String getMac() {
        return Mac;
    }

    public void setMac(String mac) {
        Mac = mac;
    }

    public String getInterface() {
        return Interface;
    }

    public void setInterface(String anInterface) {
        Interface = anInterface;
    }
}
