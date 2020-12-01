package com.broadtech.analyse.pojo.main;

/**
 * @author leo.J
 * @description 资产漏洞中间表实体类
 * @date 2020-09-23 16:36
 */
public class VulnUnify {
    private String cnvd_number;
    private String insert_time;
    private String ip;
    private String lm_number;
    private String source;

    public VulnUnify(String cnvd_number, String insert_time, String ip, String lm_number, String source) {
        this.cnvd_number = cnvd_number;
        this.insert_time = insert_time;
        this.ip = ip;
        this.lm_number = lm_number;
        this.source = source;
    }

    public String getCnvd_number() {
        return cnvd_number;
    }

    public void setCnvd_number(String cnvd_number) {
        this.cnvd_number = cnvd_number;
    }

    public String getInsert_time() {
        return insert_time;
    }

    public void setInsert_time(String insert_time) {
        this.insert_time = insert_time;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getLm_number() {
        return lm_number;
    }

    public void setLm_number(String lm_number) {
        this.lm_number = lm_number;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }
}
