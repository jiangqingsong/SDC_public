package com.broadtech.analyse.pojo.cmcc.find;

/**
 * @author leo.J
 * @description
 * @date 2020-06-14 10:59
 */
public class Ipv6Route {
    private String Dest;
    private String Mask;
    private String Proto;
    private String NextHop;
    private String Interface;

    public String getDest() {
        return Dest;
    }

    public void setDest(String dest) {
        Dest = dest;
    }

    public String getMask() {
        return Mask;
    }

    public void setMask(String mask) {
        Mask = mask;
    }

    public String getProto() {
        return Proto;
    }

    public void setProto(String proto) {
        Proto = proto;
    }

    public String getNextHop() {
        return NextHop;
    }

    public void setNextHop(String nextHop) {
        NextHop = nextHop;
    }

    public String getInterface() {
        return Interface;
    }

    public void setInterface(String anInterface) {
        Interface = anInterface;
    }
}
