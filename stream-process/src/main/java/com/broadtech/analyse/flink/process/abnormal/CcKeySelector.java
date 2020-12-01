package com.broadtech.analyse.flink.process.abnormal;

import com.broadtech.analyse.pojo.abnormal.Dpi;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @author leo.J
 * @description
 * @date 2020-08-22 16:10
 */
public class CcKeySelector implements KeySelector<Dpi, Tuple2<String, String>> {
    @Override
    public Tuple2<String, String> getKey(Dpi dpi) throws Exception {
        return Tuple2.of(dpi.getSrcIPAddress(), dpi.getDestIPAddress());
    }
}
