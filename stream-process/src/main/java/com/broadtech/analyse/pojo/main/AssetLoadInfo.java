package com.broadtech.analyse.pojo.main;

/**
 * @author leo.J
 * @description 资产导入时信息
 * @date 2020-09-23 16:16
 */
public class AssetLoadInfo {
    private String ip;
    private String osName;
    private String programName;
    private String dbName;//数据库名
    private String messageOrientedMiddlewares;//中间件信息
    private String softName;

    public AssetLoadInfo(String ip, String osName, String programName, String dbName, String messageOrientedMiddlewares, String softName) {
        this.ip = ip;
        this.osName = osName;
        this.programName = programName;
        this.dbName = dbName;
        this.messageOrientedMiddlewares = messageOrientedMiddlewares;
        this.softName = softName;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getOsName() {
        return osName;
    }

    public void setOsName(String osName) {
        this.osName = osName;
    }

    public String getProgramName() {
        return programName;
    }

    public void setProgramName(String programName) {
        this.programName = programName;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getMessageOrientedMiddlewares() {
        return messageOrientedMiddlewares;
    }

    public void setMessageOrientedMiddlewares(String messageOrientedMiddlewares) {
        this.messageOrientedMiddlewares = messageOrientedMiddlewares;
    }

    public String getSoftName() {
        return softName;
    }

    public void setSoftName(String softName) {
        this.softName = softName;
    }
}
