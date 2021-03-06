package com.broadtech.analyse.flink.window.abnormal;

import com.broadtech.analyse.pojo.abnormal.Dpi;
import org.apache.flink.shaded.curator.org.apache.curator.shaded.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * @author leo.J
 * @description 按指定条数放入一个窗口  为后面batch insert做准备
 * @date 2020-08-22 11:22
 */
public class DpiLogWindowFunc implements AllWindowFunction<Dpi, List<Dpi>, GlobalWindow> {
    @Override
    public void apply(GlobalWindow window, Iterable<Dpi> values, Collector<List<Dpi>> out) throws Exception {
        ArrayList objectNodes = Lists.newArrayList(values);
        if (objectNodes.size() > 0) {
            out.collect(objectNodes);
        }
    }
}
