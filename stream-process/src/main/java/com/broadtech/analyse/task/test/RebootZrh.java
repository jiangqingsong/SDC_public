package com.broadtech.analyse.task.test;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

/**
 * @author leo.J
 * @description
 * @date 2020-09-08 16:05
 */
public class RebootZrh {
    private static String url = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=af581581-29ab-495a-b44b-36506c4be9af";
    public static void main(String[] args) throws Exception {

        String msg = args[0];
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");//注意月份是MM
        Date date = simpleDateFormat.parse("2019-09-08 18:05:00");
        sendMsg1(msg);

    }

    public static void sendMsg() throws Exception {
        String currentTime = getCurrentTime();
        String param = "{\"msgtype\":\"text\",\"text\":{\"content\":\"现在是北京时间 " + currentTime + ", 老铁门！打起精神来！撸起袖子干！\"}}";
        HttpUtil.doPost(url, param);
    }

    public static String getCurrentTime(){
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return df.format(new Date());
    }

    public static void sendMsg1(String msg) throws Exception {
        String currentTime = getCurrentTime();
        String param = "{\"msgtype\":\"text\",\"text\":{\"content\":\"现在是北京时间 " + currentTime + ",  " + msg + "！！@所有人 \"}}";
        HttpUtil.doPost(url, param);
    }
}

class SendMsgTask extends TimerTask{

    @Override
    public void run() {
        try {
            RebootZrh.sendMsg();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
