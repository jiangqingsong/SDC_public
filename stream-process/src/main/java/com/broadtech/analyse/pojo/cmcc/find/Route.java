package com.broadtech.analyse.pojo.cmcc.find;

/**
 * @author jiangqingsong
 * @description Route表
 * @date 2020-06-12 19:19
 */
public class Route {
    private String DestMask;
    private String Proto;
    private String NextHop;
    private String Interface;

    public String getDestMask() {
        return DestMask;
    }

    public void setDestMask(String destMask) {
        DestMask = destMask;
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
