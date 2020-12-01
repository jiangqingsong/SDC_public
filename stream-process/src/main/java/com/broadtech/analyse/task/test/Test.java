package com.broadtech.analyse.task.test;

import java.util.regex.*;
/**
 * @author leo.J
 * @description
 * @date 2020-05-06 17:32
 */
public class Test {
    public static void main(String[] args) throws InterruptedException {
        //String str = "220.250.72.148|2019-12-02 00:21:14|2019-12-02 00:21:18 10.99.94.1 %%01SEC/5/ATCKDF(l):log_type=device_in_flow time=\"2019-12-02 00:21:18\" device_ip=10.99.94.1 device_type=0 direction=1 max_total_pps=373 max_total_kbps=4126 max_tcp_pps=0 max_tcp_kbps=0 max_udp_pps=373 max_udp_kbps=4126 max_icmp_pps=0 max_icmp_kbps=0 max_other_pps=0 max_other_kbps=0 avg_total_pps=5 avg_total_kbps=64 avg_tcp_pps=0 avg_tcp_kbps=0 avg_udp_pps=5 avg_udp_kbps=64 avg_icmp_pps=0 avg_icmp_kbps=0 avg_other_pps=0 avg_other_kbps=0";
        String str = "aaa,bbb,ddd";
        String pattern = "([\\s\\S]{1,},)";
        Pattern.matches(pattern, str);
        Pattern p = Pattern.compile(pattern);
        String[] split = p.split(str);
        System.out.println(split.length);
    }
}

