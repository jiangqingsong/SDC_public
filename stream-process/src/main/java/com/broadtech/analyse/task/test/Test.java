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
    public static void main(String[] args) throws InterruptedException {
        A aa = new A();
        aa.setName("AA");
        String s = JSON.toJSONString(aa);
        System.out.println(s);
    }
}

class A{
    public String name;
    public String value = "";

    public A() {
    }

    public A(String name, String value) {
        this.name = name;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
