package com.broadtech.analyse.flink.function.abnormal;

import com.broadtech.analyse.pojo.abnormal.Dpi;
import org.apache.flink.api.common.functions.FilterFunction;

import java.util.Arrays;
import java.util.List;

/**
 * @author leo.J
 * @description 过滤白名单和黑名单IP
 * @date 2020-08-21 13:54
 */
public class FilterCCIp implements FilterFunction<Dpi> {
    private String blackList;
    private String whiteList;

    public FilterCCIp(String blackList, String whiteList) {
        this.blackList = blackList;
        this.whiteList = whiteList;
    }


    @Override
    public boolean filter(Dpi dpi) throws Exception {
        List<String> blackIps = Arrays.asList(blackList.split(","));
        List<String> whiteIps = Arrays.asList(whiteList.split(","));
        String srcIPAddress = dpi.getSrcIPAddress();
        String destIPAddress = dpi.getDestIPAddress();
        if(!blackIps.contains(srcIPAddress) && !whiteIps.contains(destIPAddress)){
            return true;
        }
        if(!blackIps.contains(destIPAddress) && !whiteIps.contains(destIPAddress)){
            return true;
        }
        return false;
    }
}
