package com.broadtech.analyse.flink.sink.cmcc;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.broadtech.analyse.constants.asset.AssetDiscoverConstant;
import com.broadtech.analyse.util.db.DbUtils;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * @author leo.J
 * @description 资产发现数据入mysql
 * @date 2020-05-28 15:37
 */
public class AssetDiscoverMysqlSink extends RichSinkFunction<List<ObjectNode>> {
    PreparedStatement ps;
    BasicDataSource dataSource;
    private Connection connection;
    /**
     * @param parameters 连接信息准备
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        dataSource = new BasicDataSource();
        //connection = getConnection(dataSource);
        connection = DbUtils.getCon(dataSource, "/jdbc.properties");
        String sql = "insert into  " + AssetDiscoverConstant.TAB_NAME + "(task_id, scan_time, ip) " +
                "values (?,?,?);";
        ps = this.connection.prepareStatement(sql);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null) {
            connection.close();
        }
        if (ps != null) {
            ps.close();
        }
    }


    public void addBatch(ObjectNode objectNode) throws SQLException {
        String taskId = objectNode.get(AssetDiscoverConstant.TASK_ID).toString();
        String scanTime = objectNode.get(AssetDiscoverConstant.SCAN_TIME).toString();
        String resource_info = objectNode.get(AssetDiscoverConstant.RESOURCE_INFO).toString();
        JSONArray objects = JSON.parseArray(resource_info);
        for(Object ipObj: objects){
            String ip = ipObj.toString();
            ps.setString(1, taskId);
            ps.setString(2, scanTime);
            ps.setString(3, ip);
            ps.addBatch();
        }
    }

    /**
     * 每条数据的插入都要调用一次 invoke() 方法
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(List<ObjectNode> objectNodes, Context context) throws Exception {
        String date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());

        //遍历数据集合
        for (ObjectNode objectNode : objectNodes) {
            addBatch(objectNode);
        }
        int[] count = ps.executeBatch();//批量后执行
        System.out.println("成功了插入了" + count.length + "行数据");
    }

}
