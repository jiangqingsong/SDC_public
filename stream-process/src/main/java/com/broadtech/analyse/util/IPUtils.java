package com.broadtech.analyse.util;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.record.*;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;

/**
 * @author leo.J
 * @description
 * @date 2020-06-07 10:31
 */
public class IPUtils {
    /**
     * 获取城市
     * @param reader
     * @param ip
     * @return
     * @throws IOException
     * @throws GeoIp2Exception
     */
    public static String getCity(DatabaseReader reader, String ip) throws IOException, GeoIp2Exception {
        return reader.city(InetAddress.getByName(ip)).getCity().getNames().get("zh-CN");
    }

    /**
     * 获取省份
     * @param reader
     * @param ip
     * @return
     * @throws IOException
     * @throws GeoIp2Exception
     */
    public static String getProvince(DatabaseReader reader, String ip) throws IOException, GeoIp2Exception {
        return reader.city(InetAddress.getByName(ip)).getMostSpecificSubdivision().getNames().get("zh-CN");
    }

    /**
     * 获取国家
     * @param reader
     * @param ip
     * @return
     * @throws IOException
     * @throws GeoIp2Exception
     */
    public static String getCountry(DatabaseReader reader, String ip) throws IOException, GeoIp2Exception {
        return reader.city(InetAddress.getByName(ip)).getCountry().getNames().get("zh-CN");
    }

    public static void main(String[] args) throws Exception {

        /**
         * ipList.add("61.137.125.155");
         * ipList.add("113.204.226.35");
         * ipList.add("192.168.5.92");
         */

        String path = "D:\\SDC\\data\\CMCC\\geoip\\GeoLite2-City.mmdb";
        DatabaseReader reader = new DatabaseReader.Builder(new File(path)).build();
        InetAddress ipAddress = InetAddress.getByName("171.108.233.157");

        // 获取查询结果
        CityResponse response = reader.city(ipAddress);

        // 获取国家信息
        Country country = response.getCountry();
        System.out.println(country.getIsoCode());               // 'CN'
        System.out.println(country.getName());                  // 'China'
        System.out.println(country.getNames().get("zh-CN"));    // '中国'

        // 获取省份
        Subdivision subdivision = response.getMostSpecificSubdivision();
        System.out.println(subdivision.getName());   // 'Guangxi Zhuangzu Zizhiqu'
        System.out.println(subdivision.getIsoCode()); // '45'
        System.out.println(subdivision.getNames().get("zh-CN")); // '广西壮族自治区'

        // 获取城市
        City city = response.getCity();
        System.out.println(city.getName()); // 'Nanning'
        Postal postal = response.getPostal();
        System.out.println(postal.getCode()); // 'null'
        System.out.println(city.getNames().get("zh-CN")); // '南宁'
        Location location = response.getLocation();
        System.out.println(location.getLatitude());  // 22.8167
        System.out.println(location.getLongitude()); // 108.3167

    }
}
