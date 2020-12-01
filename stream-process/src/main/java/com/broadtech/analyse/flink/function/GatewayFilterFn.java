package com.broadtech.analyse.flink.function;

import org.apache.flink.api.common.functions.FilterFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author leo.J
 * @description
 * @date 2020-05-09 15:57
 */
public class GatewayFilterFn implements FilterFunction<String> {
    private static final Logger LOG = LoggerFactory.getLogger(GatewayFilterFn.class);
    private String separator;
    public void setSeparator(String separator) {
        this.separator = separator;
    }
    @Override
    public boolean filter(String value) throws Exception {
        boolean filterFlag = false;
        if(separator == null){
            LOG.error("This separator is null in the GatewayFilterFn!");
        }else {
            String[] split = value.split(separator);
            if(split.length != 29){
                filterFlag = true;
            }
        }
        return filterFlag;
    }
}
