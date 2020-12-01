package com.broadtech.analyse.flink.process.main;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.broadtech.analyse.constants.main.AssetLoadConstans;
import com.broadtech.analyse.pojo.main.AssetLoadInfo;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.log4j.Logger;

/**
 * @author leo.J
 * @description
 * @date 2020-09-23 16:15
 */
public class AssetLoadMapFn implements MapFunction<ObjectNode, AssetLoadInfo> {
    private static Logger LOG = Logger.getLogger(AssetLoadMapFn.class);
    @Override
    public AssetLoadInfo map(ObjectNode obj) throws Exception {
        try {
            JSONObject jsonObj = JSON.parseObject(obj.toString());
            String ip = jsonObj.get(AssetLoadConstans.IP).toString();
            String osName = jsonObj.get(AssetLoadConstans.OS_NAME).toString();
            String programName = jsonObj.get(AssetLoadConstans.PROGRAM_NAME).toString();
            String dbName = jsonObj.get(AssetLoadConstans.DB_NAME).toString();
            String messageOrientedMiddlewares = jsonObj.get(AssetLoadConstans.MESSAGE_ORIENTED_MIDDLEWARES).toString();
            String softName = jsonObj.get(AssetLoadConstans.SOFT_NAME).toString();
            AssetLoadInfo assetLoadInfo = new AssetLoadInfo(ip, osName, programName, dbName, messageOrientedMiddlewares, softName);
            return assetLoadInfo;

        }catch (Exception e){
            LOG.error("parse asset load info error! " + e);
            e.printStackTrace();
            return null;
        }

    }
}
