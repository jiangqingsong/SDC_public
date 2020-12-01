package com.broadtech.analyse.task.test;

import com.alibaba.fastjson.JSON;
import com.broadtech.analyse.pojo.main.AlarmEvenUnify;
import com.broadtech.analyse.util.TimeUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.sql.Timestamp;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Properties;
import java.util.UUID;

/**
 * @author leo.J
 * @description
 * @date 2020-08-13 17:11
 */
public class TestPro {
    public static void main(String[] args) {
        System.out.println(UUID.randomUUID().toString().replace("-", "").toLowerCase());

        T t = new T("AA", "file");
        String json = JSON.toJSONString(t);
        System.out.println(json);

        AlarmEvenUnify alarmEvenUnify = new AlarmEvenUnify(
                "test", "11", "22", "33", "4",
                "5", "192.168.5.93", "3", "1", "3",
                "huawei", "192.168.5.91", "8088", "xxxx", "192.168.5.94",
                "8081", "sssssssssss", "3", "南京", "98.2",
                "", "2020-09-10 11:00:19", "", "", ""
        );
        System.out.println(JSON.toJSONString(alarmEvenUnify));
    }

}
class T{
    private String name;
    private String file;

    public T(String name, String file) {
        this.name = name;
        this.file = file;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getFile() {
        return file;
    }

    public void setFile(String file) {
        this.file = file;
    }
}
