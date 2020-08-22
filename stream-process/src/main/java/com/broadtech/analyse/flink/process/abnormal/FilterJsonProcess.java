package com.broadtech.analyse.flink.process.abnormal;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author jiangqingsong
 * @description
 * @date 2020-08-22 13:48
 */
public class FilterJsonProcess extends ProcessFunction<ObjectNode, ObjectNode> {
    private static final OutputTag<ObjectNode> jsonFormatTag = new OutputTag<ObjectNode>("JsonFormat") {};
    @Override
    public void processElement(ObjectNode value, Context ctx, Collector<ObjectNode> out) throws Exception {
        if (!value.has("jsonParseError")) {
            ctx.output(jsonFormatTag, value);
        }
    }
}
