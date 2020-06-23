package com.broadtech.analyse.util;

import com.broadtech.analyse.pojo.user.UserAgent;
import cz.mallat.uasparser.OnlineUpdater;
import cz.mallat.uasparser.UASparser;
import cz.mallat.uasparser.UserAgentInfo;

import java.io.IOException;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * @author 吴榜元
 * @create 2020-05-11 13:45
 **/
public class UserAgentUtil {
    private static final String ANDROID = "Android";
    private static final String IPHONE = "iPhone";
    private static final String IPAD = "iPad";
    private static final String NO = "未知";


    /**
     * 根据正则抽取字符串集合
     *
     * @param str 原始字符串
     * @param rex 正则表达式
     * @return 抽取出来的字符串集合
     */
    public static String extract(String str, String rex) {
        if (Objects.isNull(str) || str.isEmpty()) return str;
        Pattern pattern = Pattern.compile(rex);
        Matcher matcher = pattern.matcher(str);
        return matcher.find() ? matcher.group(0) : "";
    }
    /**
     * 获取解析userAgent类
     *
     * @return UserAgentInfo
     */
    private static UserAgentInfo getUserAgentInfo(String line) {
        UserAgentInfo parse = null;
        try {
            UASparser uaSparser = new UASparser(OnlineUpdater.getVendoredInputStream());
            parse = uaSparser.parse(line);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return parse;
    }


    /**
     * 获取移动用户操作系统
     *
     * @param userAgent 数据
     * @return 操作系统类型
     */
    private static String getMobileOS(String userAgent) {
        if (userAgent.contains(ANDROID)) return ANDROID;
        else if (userAgent.contains(IPHONE)) return IPHONE;
        else if (userAgent.contains(IPAD)) return IPAD;
        else return NO;
    }

    /**
     * 获取设备类型
     *
     * @param userAgent userAgent数据
     * @return 设备类型
     */
    private static String getPhoneModel(String userAgent) {
        if (null == userAgent || "".equals(userAgent)) return NO;
        String mobileOS = getMobileOS(userAgent);
        switch (mobileOS) {
            case ANDROID:
                String rex = "[()]+";
                String[] str = userAgent.split(rex);
                str = str[1].split("[;]");
                String[] res = str[str.length - 1].split("Build/");
                return res[0];
            case IPHONE:
                return IPHONE;
            case IPAD:
                return IPAD;
            default:
                return NO;
        }
    }

    /**
     * 获取系统版本号
     *
     * @param versionStr 获取的系统信息
     * @return 系统版本号
     */
    private static String getOsVersion(String versionStr) {
        if (versionStr == null || versionStr.equals("")) return NO;
        //正则匹配版本号
        String reg = "\\d+(\\.\\d+)*";
        String version = extract(versionStr, reg);
        if ("".equals(version) || version == null) {
            return NO;
        }
        String osVersion;
        if (version.endsWith(".")) {
            osVersion = version.substring(0, version.length() - 1);
        } else {
            osVersion = version;
        }
        return osVersion;
    }

    /**
     * 解析userAgent的数据方法
     * @param userAgentData 原始数据行
     * @return UserAgent对象
     */
    public static UserAgent proUserAgent(String userAgentData) {
        UserAgentInfo userAgentInfo = UserAgentUtil.getUserAgentInfo(userAgentData);
        if (userAgentInfo == null) return null;
        //获取设备类型
        String deviceModel = UserAgentUtil.getPhoneModel(userAgentData);
        //获取系统名称
        String os;
        if (UserAgentInfo.UNKNOWN.equals(userAgentInfo.getOsFamily())) {
            os = NO;
        } else {
            os = userAgentInfo.getOsFamily();
        }
        String osOsName = userAgentInfo.getOsName();
        //获取器系统版本号
        String osVer = UserAgentUtil.getOsVersion(osOsName);
        //获取浏览器名称
        String browser;
        if (UserAgentInfo.UNKNOWN.equals(userAgentInfo.getUaFamily())) {
            browser = NO;
        } else {
            browser = userAgentInfo.getUaFamily();
        }
        //获取浏览器版本
        String browserVer = UserAgentUtil.getOsVersion(userAgentInfo.getBrowserVersionInfo());
        return new UserAgent(deviceModel, os, osVer, browser, browserVer);
    }

}
