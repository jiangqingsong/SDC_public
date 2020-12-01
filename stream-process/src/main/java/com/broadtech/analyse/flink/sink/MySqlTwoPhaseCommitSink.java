package com.broadtech.analyse.flink.sink;

import com.alibaba.fastjson.JSONObject;
import com.broadtech.analyse.constants.asset.DpiInfoConstant;
import com.broadtech.analyse.util.DBConnectUtil;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author leo.J
 * @description 自定义kafka to mysql,继承TwoPhaseCommitSinkFunction,实现两阶段提交
 * @date 2020-05-28 14:19
 */
public class MySqlTwoPhaseCommitSink extends TwoPhaseCommitSinkFunction<ObjectNode, Connection,Void> {
    private static final Logger log = LoggerFactory.getLogger(MySqlTwoPhaseCommitSink.class);

    public MySqlTwoPhaseCommitSink(){
        super(new KryoSerializer<>(Connection.class,new ExecutionConfig()), VoidSerializer.INSTANCE);
    }

    /**
     * 执行数据库入库操作  task初始化的时候调用
     * @param connection
     * @param objectNode
     * @param context
     * @throws Exception
     */
    @Override
    protected void invoke(Connection connection, ObjectNode objectNode, Context context) throws Exception {
        log.info("start invoke...");
        String date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
        log.info("===>date:" + date + " " + objectNode);
        //log.info("===>date:{} --{}",date,objectNode);
        String value = objectNode.get("value").toString();
        log.info("objectNode-value:" + value);

        //获取json 各字段数据信息
        JSONObject dpiInfo = JSONObject.parseObject(value);
        String SRC_IP_TYPE = dpiInfo.getString(DpiInfoConstant.SRC_IP_TYPE);
        String DST_IP_TYPE = dpiInfo.getString(DpiInfoConstant.DST_IP_TYPE);
        String SRC_IP = dpiInfo.getString(DpiInfoConstant.SRC_IP);
        String SRC_PORT = dpiInfo.getString(DpiInfoConstant.SRC_PORT);
        String SRC_DWMC = dpiInfo.getString(DpiInfoConstant.SRC_DWMC);
        String DST_IP = dpiInfo.getString(DpiInfoConstant.DST_IP);
        String DST_PORT = dpiInfo.getString(DpiInfoConstant.DST_PORT);
        String DST_DWMC = dpiInfo.getString(DpiInfoConstant.DST_DWMC);
        String UL_TRAFFIC = dpiInfo.getString(DpiInfoConstant.UL_TRAFFIC);
        String DL_TRAFFIC = dpiInfo.getString(DpiInfoConstant.DL_TRAFFIC);
        String UL_PACKTES = dpiInfo.getString(DpiInfoConstant.UL_PACKTES);
        String DL_PACKETS = dpiInfo.getString(DpiInfoConstant.DL_PACKETS);
        String L4PROTO = dpiInfo.getString(DpiInfoConstant.L4PROTO);
        String PROTOCOL = dpiInfo.getString(DpiInfoConstant.PROTOCOL);
        String MANUFACTURER = dpiInfo.getString(DpiInfoConstant.MANUFACTURER);
        String ASSET_TYPE = dpiInfo.getString(DpiInfoConstant.ASSET_TYPE);
        String START_TIME = dpiInfo.getString(DpiInfoConstant.START_TIME);
        String END_TIME = dpiInfo.getString(DpiInfoConstant.END_TIME);
        String REMARKS = dpiInfo.getString(DpiInfoConstant.REMARKS);
        String HOST = dpiInfo.getString(DpiInfoConstant.HOST);
        String URI = dpiInfo.getString(DpiInfoConstant.URI);
        String DEV_IP = dpiInfo.getString(DpiInfoConstant.DEV_IP);
        String DEV_PORT = dpiInfo.getString(DpiInfoConstant.DEV_PORT);
        String IP_TYPE = dpiInfo.getString(DpiInfoConstant.IP_TYPE);
        String DEV_TYPE = dpiInfo.getString(DpiInfoConstant.DEV_TYPE);
        String DEV_NAME = dpiInfo.getString(DpiInfoConstant.DEV_NAME);
        String SOFT_NAME = dpiInfo.getString(DpiInfoConstant.SOFT_NAME);
        String SOFT_VER = dpiInfo.getString(DpiInfoConstant.SOFT_VER);
        String VENDOR = dpiInfo.getString(DpiInfoConstant.VENDOR);
        String OS = dpiInfo.getString(DpiInfoConstant.OS);
        String OS_VER = dpiInfo.getString(DpiInfoConstant.OS_VER);
        String NET_TYPE = dpiInfo.getString(DpiInfoConstant.NET_TYPE);
        String LAST_TIME = dpiInfo.getString(DpiInfoConstant.LAST_TIME);
        String SRC_MAC = dpiInfo.getString(DpiInfoConstant.SRC_MAC);
        String DST_MAC = dpiInfo.getString(DpiInfoConstant.DST_MAC);


        String sql = "insert into dpilog (,dst_ip_type,src_ip,src_port,src_dwmc,dst_ip,dst_port,dst_dwmc,ul_traffic,dl_traffic,ul_packtes,dl_packets,l4proto,protocol,manufacturer,asset_type,start_time,end_time,remarks,host,uri,dev_ip,dev_port,ip_type,dev_type,dev_name,sof_tname,sof_tver,vendor,os,os_ver,net_type,last_time,src_mac,dst_mac) " +
                "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        PreparedStatement ps = connection.prepareStatement(sql);
        ps.setString(1,SRC_IP_TYPE);
        ps.setString(2,DST_IP_TYPE);
        ps.setString(3,SRC_IP);
        ps.setString(4,SRC_PORT);
        ps.setString(5,SRC_DWMC);
        ps.setString(6,DST_IP);
        ps.setString(7,DST_PORT);
        ps.setString(8,DST_DWMC);
        ps.setString(9,UL_TRAFFIC);
        ps.setString(10,DL_TRAFFIC);
        ps.setString(11,UL_PACKTES);
        ps.setString(12,DL_PACKETS);
        ps.setString(13,L4PROTO);
        ps.setString(14,PROTOCOL);
        ps.setString(15,MANUFACTURER);
        ps.setString(16,ASSET_TYPE);
        ps.setString(17,START_TIME);
        ps.setString(18,END_TIME);
        ps.setString(19,REMARKS);
        ps.setString(20,HOST);
        ps.setString(21,URI);
        ps.setString(22,DEV_IP);
        ps.setString(23,DEV_PORT);
        ps.setString(24,IP_TYPE);
        ps.setString(25,DEV_TYPE);
        ps.setString(26,DEV_NAME);
        ps.setString(27,SOFT_NAME);
        ps.setString(28,SOFT_VER);
        ps.setString(29,VENDOR);
        ps.setString(30,OS);
        ps.setString(31,OS_VER);
        ps.setString(32,NET_TYPE);
        ps.setString(33,LAST_TIME);
        ps.setString(34,SRC_MAC);
        ps.setString(35,DST_MAC);
        Timestamp value_time = new Timestamp(System.currentTimeMillis());
        //执行insert语句
        ps.execute();
        //手动制造异常
        /*if(Integer.parseInt(value_str) == 15) {
            System.out.println(1 / 0);
        }*/
    }

    /**
     * 获取连接，开启手动提交事物（getConnection方法中）
     * @return
     * @throws Exception
     */
    @Override
    protected Connection beginTransaction() throws Exception {
        log.info("start beginTransaction.......");
        String url = "jdbc:mysql://master02:3306/sdc?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&useSSL=false&autoReconnect=true";
        Connection connection = DBConnectUtil.getConnection(url, "root", "broadtech");
        return connection;
    }

    /**
     *预提交，这里预提交的逻辑在invoke方法中
     * @param connection
     * @throws Exception
     */
    @Override
    protected void preCommit(Connection connection) throws Exception {
        log.info("start preCommit...");
    }

    /**
     * 如果invoke方法执行正常，则提交事务
     * @param connection
     */
    @Override
    protected void commit(Connection connection) {
        log.info("start commit...");
        DBConnectUtil.commit(connection);
    }

    /**
     * 如果invoke执行异常则回滚事物，下一次的checkpoint操作也不会执行
     * @param connection
     */
    @Override
    protected void abort(Connection connection) {
        log.info("start abort rollback...");
        DBConnectUtil.rollback(connection);
    }
}
