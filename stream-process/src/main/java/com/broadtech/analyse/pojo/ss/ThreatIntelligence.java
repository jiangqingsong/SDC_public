package com.broadtech.analyse.pojo.ss;

/**
 * @author leo.J
 * @description 威胁情报库POJO
 * @date 2020-07-20 13:45
 */
public class ThreatIntelligence {
    private String geo;
    private String reputation;
    private String type;
    private String value;

    public ThreatIntelligence(String geo, String reputation, String type, String value) {
        this.geo = geo;
        this.reputation = reputation;
        this.type = type;
        this.value = value;
    }

    public String getGeo() {
        return geo;
    }

    public void setGeo(String geo) {
        this.geo = geo;
    }

    public String getReputation() {
        return reputation;
    }

    public void setReputation(String reputation) {
        this.reputation = reputation;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
