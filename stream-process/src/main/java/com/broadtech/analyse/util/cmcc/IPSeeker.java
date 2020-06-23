package com.broadtech.analyse.util.cmcc;

/**
 * @author mingcong.li01@hand-china.com
 * @version 1.0
 * @ClassName IPSeeker
 * @description
 * @date 2020/4/2 11:07
 * @since JDK 1.8
 */
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;


import com.maxmind.geoip2.DatabaseReader;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.net.util.IPAddressUtil;

public class IPSeeker {

    private static final Logger logger = LoggerFactory.getLogger(IPSeeker.class);

    //纯真IP数据库名
    private String IP_FILE = "";
    //保存的文件夹
    private String INSTALL_DIR = "";

    // 一些固定常量，比如记录长度等等
    private static final int IP_RECORD_LENGTH = 7;
    private static final byte REDIRECT_MODE_1 = 0x01;
    private static final byte REDIRECT_MODE_2 = 0x02;

    // 随机文件访问类
    private RandomAccessFile ipFile;
    // 起始地区的开始和结束的绝对偏移
    private long ipBegin, ipEnd;
    // 为提高效率而采用的临时变量
    private IPLocation loc;
    private byte[] buf;
    private byte[] b4;
    private byte[] b3;

    public Map<String,Object> getGPSInfo(String ip) throws Exception {
        File database = new File(this.getClass().getClassLoader().getResource("geoip2/GeoLite2-City.mmdb").getPath());
        DatabaseReader reader = null;
        Map<String,Object> data = new HashMap<>();
        String country = null;
        try {
            reader = new DatabaseReader.Builder(database).build();
            IPLocation ipl = getIPLocation(ip);
            //经纬度如果获取-1.00则说明获取失败
            data.put("longitude",IPUtils.getLongitude(reader,ip));
            data.put("latitude",IPUtils.getLatitude(reader,ip));
            country = IPUtils.getCountry(reader,ip);
            if(-1.00D == Double.valueOf(data.get("longitude")+"")&&-1.00D == Double.valueOf(data.get("latitude")+"")){
                data.put("country","NOT FOUNT");
                data.put("city","NOT FOUNT");
                data.put("province","NOT FOUNT");
            }else if(null ==  country||StringUtils.isEmpty(country)){
                data.put("country",ipl.getCountry());
            }else{
                data.put("country",country);
                data.put("city",IPUtils.getCity(reader,ip));
                data.put("province",IPUtils.getProvince(reader,ip));
            }
            data.put("area",ipl.getArea());
            data.put("ip",ip);
        } catch (Exception e) {
            throw new Exception("IP地址解析失败！");
        }

        return data;
    }

    public IPSeeker()  {
        //文件名（qqwry.dat），保存在properties文件中
        this.IP_FILE = "qqwry.dat";
        //qqwry.dat所在目录，保存在properties文件中
        this.INSTALL_DIR = "ipLocationInfo/qqwry.dat";
        loc = new IPLocation();
        buf = new byte[100];
        b4 = new byte[4];
        b3 = new byte[3];
        try {
            URL url = this.getClass().getClassLoader().getResource("ipLocationInfo/qqwry.dat");
            ipFile = new RandomAccessFile(url.getPath() , "r");
        } catch (FileNotFoundException e) {
            // 如果找不到这个文件，再尝试再当前目录下搜索，这次全部改用小写文件名
            //     因为有些系统可能区分大小写导致找不到ip地址信息文件
            String filename = new File(IP_FILE).getName().toLowerCase();
            File[] files = new File(INSTALL_DIR).listFiles();
            for(int i = 0; i < files.length; i++) {
                if(files[i].isFile()) {
                    if(files[i].getName().toLowerCase().equals(filename)) {
                        try {
                            ipFile = new RandomAccessFile(files[i], "r");
                        } catch (FileNotFoundException e1) {
                            logger.error("IP地址信息文件没有找到，IP显示功能将无法使用",e1);
                            ipFile = null;
                        }
                        break;
                    }
                }
            }
        }
        // 如果打开文件成功，读取文件头信息
        if(ipFile != null) {
            try {
                ipBegin = readLong4(0);
                ipEnd = readLong4(4);
                if(ipBegin == -1 || ipEnd == -1) {
                    ipFile.close();
                    ipFile = null;
                }
            } catch (IOException e) {
                logger.error("IP地址信息文件格式有错误，IP显示功能将无法使用",e);
                ipFile = null;
            }
        }
    }

    public IPLocation getIPLocation(String ip){
        IPLocation location = getIPLocation(getIpByteArrayFromString(ip));
        return location;
    }

    public boolean internalIp(String ip) {
        byte[] addr = IPAddressUtil.textToNumericFormatV4(ip);
        return internalIp(addr);
    }


    public boolean internalIp(byte[] addr) {
        final byte b0 = addr[0];
        final byte b1 = addr[1];
        //10.x.x.x/8
        final byte SECTION_1 = 0x0A;
        //172.16.x.x/12
        final byte SECTION_2 = (byte) 0xAC;
        final byte SECTION_3 = (byte) 0x10;
        final byte SECTION_4 = (byte) 0x1F;
        //192.168.x.x/16
        final byte SECTION_5 = (byte) 0xC0;
        final byte SECTION_6 = (byte) 0xA8;
        switch (b0) {
            case SECTION_1:
                return true;
            case SECTION_2:
                if (b1 >= SECTION_3 && b1 <= SECTION_4) {
                    return true;
                }
            case SECTION_5:
                switch (b1) {
                    case SECTION_6:
                        return true;
                }
            default:
                return false;

        }
    }

    /**
     * 根据ip搜索ip信息文件，得到IPLocation结构，所搜索的ip参数从类成员ip中得到
     * @param ip 要查询的IP
     * @return IPLocation结构
     */
    private IPLocation getIPLocation(byte[] ip) {
        IPLocation info = null;
        long offset = locateIP(ip);
        if(offset != -1) {
            info = getIPLocation(offset);
        }
        if(info == null) {
            info = new IPLocation();
            info.setCountry(Message.unknown_country);
            info.setArea(Message.unknown_area);
        }
        return info;
    }

    /**
     * 从offset位置读取4个字节为一个long，因为java为big-endian格式，所以没办法
     * 用了这么一个函数来做转换
     * @param offset
     * @return 读取的long值，返回-1表示读取文件失败
     */
    private long readLong4(long offset) {
        long ret = 0;
        try {
            ipFile.seek(offset);
            ret |= (ipFile.readByte() & 0xFF);
            ret |= ((ipFile.readByte() << 8) & 0xFF00);
            ret |= ((ipFile.readByte() << 16) & 0xFF0000);
            ret |= ((ipFile.readByte() << 24) & 0xFF000000);
            return ret;
        } catch (IOException e) {
            return -1;
        }
    }

    /**
     * 从offset位置读取3个字节为一个long，因为java为big-endian格式，所以没办法
     * 用了这么一个函数来做转换
     * @param offset 整数的起始偏移
     * @return 读取的long值，返回-1表示读取文件失败
     */
    private long readLong3(long offset) {
        long ret = 0;
        try {
            ipFile.seek(offset);
            ipFile.readFully(b3);
            ret |= (b3[0] & 0xFF);
            ret |= ((b3[1] << 8) & 0xFF00);
            ret |= ((b3[2] << 16) & 0xFF0000);
            return ret;
        } catch (IOException e) {
            return -1;
        }
    }

    /**
     * 从当前位置读取3个字节转换成long
     * @return 读取的long值，返回-1表示读取文件失败
     */
    private long readLong3() {
        long ret = 0;
        try {
            ipFile.readFully(b3);
            ret |= (b3[0] & 0xFF);
            ret |= ((b3[1] << 8) & 0xFF00);
            ret |= ((b3[2] << 16) & 0xFF0000);
            return ret;
        } catch (IOException e) {
            return -1;
        }
    }

    /**
     * 从offset位置读取四个字节的ip地址放入ip数组中，读取后的ip为big-endian格式，但是
     * 文件中是little-endian形式，将会进行转换
     * @param offset
     * @param ip
     */
    private void readIP(long offset, byte[] ip) {
        try {
            ipFile.seek(offset);
            ipFile.readFully(ip);
            byte temp = ip[0];
            ip[0] = ip[3];
            ip[3] = temp;
            temp = ip[1];
            ip[1] = ip[2];
            ip[2] = temp;
        } catch (IOException e) {
            logger.error("",e);
        }
    }

    /**
     * 把类成员ip和beginIp比较，注意这个beginIp是big-endian的
     * @param ip 要查询的IP
     * @param beginIp 和被查询IP相比较的IP
     * @return 相等返回0，ip大于beginIp则返回1，小于返回-1。
     */
    private int compareIP(byte[] ip, byte[] beginIp) {
        for(int i = 0; i < 4; i++) {
            int r = compareByte(ip[i], beginIp[i]);
            if(r != 0) {
                return r;
            }
        }
        return 0;
    }

    /**
     * 把两个byte当作无符号数进行比较
     * @param b1
     * @param b2
     * @return 若b1大于b2则返回1，相等返回0，小于返回-1
     */
    private int compareByte(byte b1, byte b2) {
        if((b1 & 0xFF) > (b2 & 0xFF)) { // 比较是否大于
            return 1;
        } else if((b1 ^ b2) == 0) {// 判断是否相等
            return 0;
        } else {
            return -1;
        }
    }

    /**
     * 这个方法将根据ip的内容，定位到包含这个ip国家地区的记录处，返回一个绝对偏移
     * 方法使用二分法查找。
     * @param ip 要查询的IP
     * @return 如果找到了，返回结束IP的偏移，如果没有找到，返回-1
     */
    private long locateIP(byte[] ip) {
        long m = 0;
        int r;
        // 比较第一个ip项
        readIP(ipBegin, b4);
        r = compareIP(ip, b4);
        if(r == 0) {
            return ipBegin;
        } else if(r < 0) {
            return -1;
        }
        // 开始二分搜索
        for(long i = ipBegin, j = ipEnd; i < j; ) {
            m = getMiddleOffset(i, j);
            readIP(m, b4);
            r = compareIP(ip, b4);
            // log.debug(Utils.getIpStringFromBytes(b));
            if(r > 0) {
                i = m;
            } else if(r < 0) {
                if(m == j) {
                    j -= IP_RECORD_LENGTH;
                    m = j;
                } else {
                    j = m;
                }
            } else {
                return readLong3(m + 4);
            }
        }
        // 如果循环结束了，那么i和j必定是相等的，这个记录为最可能的记录，但是并非
        // 肯定就是，还要检查一下，如果是，就返回结束地址区的绝对偏移
        m = readLong3(m + 4);
        readIP(m, b4);
        r = compareIP(ip, b4);
        if(r <= 0) {
            return m;
        } else {
            return -1;
        }
    }

    /**
     * 得到begin偏移和end偏移中间位置记录的偏移
     * @param begin
     * @param end
     * @return
     */
    private long getMiddleOffset(long begin, long end) {
        long records = (end - begin) / IP_RECORD_LENGTH;
        records >>= 1;
        if(records == 0) {
            records = 1;
        }
        return begin + records * IP_RECORD_LENGTH;
    }

    /**
     * 给定一个ip国家地区记录的偏移，返回一个IPLocation结构
     * @param offset 国家记录的起始偏移
     * @return IPLocation对象
     */
    private IPLocation getIPLocation(long offset) {
        try {
            // 跳过4字节ip
            ipFile.seek(offset + 4);
            // 读取第一个字节判断是否标志字节
            byte b = ipFile.readByte();
            if(b == REDIRECT_MODE_1) {
                // 读取国家偏移
                long countryOffset = readLong3();
                // 跳转至偏移处
                ipFile.seek(countryOffset);
                // 再检查一次标志字节，因为这个时候这个地方仍然可能是个重定向
                b = ipFile.readByte();
                if(b == REDIRECT_MODE_2) {
                    loc.setCountry(readString(readLong3()));
                    ipFile.seek(countryOffset + 4);
                } else {
                    loc.setCountry(readString(countryOffset));
                }
                // 读取地区标志
                loc.setArea(readArea(ipFile.getFilePointer()));
            } else if(b == REDIRECT_MODE_2) {
                loc.setCountry(readString(readLong3()));
                loc.setArea(readArea(offset + 8));
            } else {
                loc.setCountry(readString(ipFile.getFilePointer() - 1));
                loc.setArea( readArea(ipFile.getFilePointer()));
            }
            return loc;
        } catch (IOException e) {
            return null;
        }
    }

    /**
     * 从offset偏移开始解析后面的字节，读出一个地区名
     * @param offset 地区记录的起始偏移
     * @return 地区名字符串
     * @throws IOException
     */
    private String readArea(long offset) throws IOException {
        ipFile.seek(offset);
        byte b = ipFile.readByte();
        if(b == REDIRECT_MODE_1 || b == REDIRECT_MODE_2) {
            long areaOffset = readLong3(offset + 1);
            if(areaOffset == 0) {
                return Message.unknown_area;
            } else {
                return readString(areaOffset);
            }
        } else {
            return readString(offset);
        }
    }

    /**
     * 从offset偏移处读取一个以0结束的字符串
     * @param offset 字符串起始偏移
     * @return 读取的字符串，出错返回空字符串
     */
    private String readString(long offset) {
        try {
            ipFile.seek(offset);
            int i;
            for(i = 0, buf[i] = ipFile.readByte(); buf[i] != 0; buf[++i] = ipFile.readByte()) {};
            if(i != 0) {
                return getString(buf, 0, i, "GBK");
            }
        } catch (IOException e) {
            logger.error("",e);
        }
        return "";
    }

    /**
     * 从ip的字符串形式得到字节数组形式
     * @param ip 字符串形式的ip
     * @return 字节数组形式的ip
     */
    private static byte[] getIpByteArrayFromString(String ip) {
        byte[] ret = new byte[4];
        StringTokenizer st = new StringTokenizer(ip, ".");
        try {
            ret[0] = (byte)(Integer.parseInt(st.nextToken()) & 0xFF);
            ret[1] = (byte)(Integer.parseInt(st.nextToken()) & 0xFF);
            ret[2] = (byte)(Integer.parseInt(st.nextToken()) & 0xFF);
            ret[3] = (byte)(Integer.parseInt(st.nextToken()) & 0xFF);
        } catch (Exception e) {
            logger.error("从ip的字符串形式得到字节数组形式报错", e);
        }
        return ret;
    }

    /**
     * 根据某种编码方式将字节数组转换成字符串
     * @param b 字节数组
     * @param offset 要转换的起始位置
     * @param len 要转换的长度
     * @param encoding 编码方式
     * @return 如果encoding不支持，返回一个缺省编码的字符串
     */
    private static String getString(byte[] b, int offset, int len, String encoding) {
        try {
            return new String(b, offset, len, encoding);
        } catch (UnsupportedEncodingException e) {
            return new String(b, offset, len);
        }
    }

    public static String getRegionByIpadr(String ipaddress) throws Exception {
        IPSeeker ipSeeker = new IPSeeker();
        Map<String, Object> gpsInfo = ipSeeker.getGPSInfo(ipaddress);
        String country = null;
        String province = null;
        String city = null;
        String area = null;
        String longitude = null;
        String latitude = null;
        //获取国家
        Object country1 = gpsInfo.get("country");
        if (country1 != null) {
            country = gpsInfo.get("country").toString();
        }else {
            country = "NOT FOUNT";
        }

        //获取省份
        Object province1 = gpsInfo.get("province");
        if (province1 != null) {
            province = province1.toString();
        }else{
            province = "NOT FOUNT";
        }

        //获取城市
        Object city1 = gpsInfo.get("city");
        if (city1 != null) {
            city = city1.toString();
        }else{
            city = "NOT FOUNT";
        }
        //获取地区
        Object area1 = gpsInfo.get("area");
        if (area1 != null) {
            area = area1.toString();
        }else {
            area = "NOT FOUNT";
        }
        //获取经度和纬度
        Object longitude1 = gpsInfo.get("longitude");
        if (longitude1 != null){
            longitude = longitude1.toString();
        }else{
            longitude = "NOT FOUNT";
        }
        Object latitude1 = gpsInfo.get("latitude");
        if (latitude1 != null){
            latitude = latitude1.toString();
        }else{
            latitude = "NOT FOUNT";
        }

        if (country.equals("NOT FOUNT") || province.equals("NOT FOUNT") || city.equals("NOT FOUNT")){
            System.out.println(ipaddress + "is neibu network ip address,so can not get area info.");
        } else {
            System.out.println(country + province + city+ area);
        }
        return country + province + city + area;
    }
    public static void main(String[] args) {
        try {
            String regionByIpadr = IPSeeker.getRegionByIpadr("202.102.213.68");
            System.out.println(regionByIpadr);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}