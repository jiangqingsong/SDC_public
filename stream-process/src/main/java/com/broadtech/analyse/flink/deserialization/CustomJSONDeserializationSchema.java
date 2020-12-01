package com.broadtech.analyse.flink.deserialization;

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;

/**
 * @author leo.J
 * @description 自定义JSON反序列化，解决非json传输出错问题
 * @date 2020-06-05 15:58
 */
public class CustomJSONDeserializationSchema extends AbstractDeserializationSchema<ObjectNode> {
    private ObjectMapper mapper;
    @Override
    public ObjectNode deserialize(byte[] message) throws IOException {
        if (mapper == null) {
            mapper = new ObjectMapper();
        }

        ObjectNode objectNode;
        try {
            objectNode = mapper.readValue(message, ObjectNode.class);
        } catch (Exception e) {
            //给json解析出错记录标记，后续可以做过滤
            ObjectMapper errorMapper = new ObjectMapper();
            ObjectNode errorObjectNode = errorMapper.createObjectNode();
            errorObjectNode.put("jsonParseError", new String(message));
            objectNode = errorObjectNode;
        }
        return objectNode;
    }

    @Override
    public boolean isEndOfStream(ObjectNode nextElement) {
        return false;
    }

}
