package com.broadtech.analyse.util.cmcc;

/**
 * @author mingcong.li01@hand-china.com
 * @version 1.0
 * @ClassName IPUtils
 * @description
 * @date 2020/4/2 15:05
 * @since JDK 1.8
 */

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.model.CityResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;

/**
 *
 * @description: 获取客户端IP地址
 * @author: paulandcode
 * @email: paulandcode@gmail.com
 * @since: 2018年9月17日 下午3:44:51
 */
public class IPUtils {
    private static Logger logger = LoggerFactory.getLogger(IPUtils.class);

    /**
     *
     * @description: 获得国家
     * @param reader
     * @param ip
     * @return
     * @throws Exception
     */
   /* public static String getCountry(DatabaseReader reader, String ip) throws Exception {
        return reader.city(InetAddress.getByName(ip)).getCountry().getNames().get("zh-CN");
    }*/
    public static String getCountry(DatabaseReader reader, String ip) throws Exception {
        CityResponse cityResponse = null;
        try{
            cityResponse = reader.city(InetAddress.getByName(ip));
        }catch (Exception e){
            return "";
        }
        if(null == cityResponse){
            return "";
        }
        return reader.city(InetAddress.getByName(ip)).getCountry().getNames().get("zh-CN");
    }

    /**
     *
     * @description: 获得省份
     * @param reader
     * @param ip
     * @return
     * @throws Exception
     */
    public static String getProvince(DatabaseReader reader, String ip) throws Exception {
        return reader.city(InetAddress.getByName(ip)).getMostSpecificSubdivision().getNames().get("zh-CN");
    }

    /**
     *
     * @description: 获得城市
     * @param reader
     * @param ip
     * @return
     * @throws Exception
     */
    public static String getCity(DatabaseReader reader, String ip) throws Exception {
        return reader.city(InetAddress.getByName(ip)).getCity().getNames().get("zh-CN");
    }

    /**
     *
     * @description: 获得经度
     * @param reader
     * @param ip
     * @return
     * @throws Exception
     */
    public static Double getLongitude(DatabaseReader reader, String ip) throws Exception {
        CityResponse cityResponse = null;
        try{
            cityResponse = reader.city(InetAddress.getByName(ip));
        }catch (Exception e){
            return -1.00D;
        }
        if(null == cityResponse){
            return -1.00D;
        }
        return cityResponse.getLocation().getLongitude();
    }

    /**
     *
     * @description: 获得纬度
     * @param reader
     * @param ip
     * @return
     * @throws Exception
     */
    public static Double getLatitude(DatabaseReader reader, String ip) throws Exception {
        CityResponse cityResponse = null;
        try{
            cityResponse = reader.city(InetAddress.getByName(ip));
        }catch (Exception e){
            return -1.00D;
        }
        if(null == cityResponse){
            return -1.00D;
        }
        return cityResponse.getLocation().getLatitude();
    }
}
