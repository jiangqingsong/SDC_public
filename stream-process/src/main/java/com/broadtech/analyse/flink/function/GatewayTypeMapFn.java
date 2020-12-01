package com.broadtech.analyse.flink.function;

import com.broadtech.analyse.pojo.gateway.GatewaySession;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @author leo.J
 * @description 原始数据转换成GatewaySession
 * @date 2020-05-09 15:54
 */
public class GatewayTypeMapFn implements MapFunction<String, GatewaySession> {

    private String separator;
    public void setSeparator(String separator) {
        this.separator = separator;
    }
    @Override
    public GatewaySession map(String value) throws Exception {
        String[] split = value.split(separator);
        String riskTypeEn = split[22];
        String userAgent = split[27];

        //1、风险类型（risk_type）翻译 && risk_level
        String riskType;
        String riskLevel;
        switch (riskTypeEn){
            case "malicious": {
                riskType = "恶意站点";
                riskLevel = "高";
            }
            case "malware": {
                riskType = "恶意站点";
                riskLevel = "高";
            }
            case "mining ": {
                riskType = "挖矿站点";
                riskLevel = "高";
            }
            case "phishing ": {
                riskType = "网络钓鱼站点";
                riskLevel = "高";
            }
            case "spam ": {
                riskType = "垃圾邮件站点";
                riskLevel = "低";
            }
            case "suspicious  ": {
                riskType = "可疑站点";
                riskLevel = "中";
            }
            default: {
                riskType = "";
                riskLevel = "";
            }
        }

        //2、user_agent 解析回填


        //3、风险等级映射

        //4、增加reject_reason

        //5、增加device_type

        GatewaySession gatewaySession = new GatewaySession();

        return gatewaySession;
    }
}
