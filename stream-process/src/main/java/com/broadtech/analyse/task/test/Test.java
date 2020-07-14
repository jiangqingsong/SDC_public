package com.broadtech.analyse.task.test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.broadtech.analyse.constants.asset.AgentCollectConstant;
import com.broadtech.analyse.pojo.cmcc.ResMessage;
import com.broadtech.analyse.pojo.cmcc.Vulnerability;
import com.broadtech.analyse.util.TimeUtils;
import org.apache.commons.compress.utils.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;

import java.sql.Timestamp;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * @author jiangqingsong
 * @description
 * @date 2020-05-06 17:32
 */
public class Test {
    public static void main(String[] args) {

        String time = "2020-06-23 14:15:26";
        long timestamp = TimeUtils.getTimestamp("yyyy-MM-dd HH:mm:ss", time);
        System.out.println(timestamp);

        List<String> list = Arrays.asList("127.0.0.1", "localhost", "192.168.5.93");

    }
}
