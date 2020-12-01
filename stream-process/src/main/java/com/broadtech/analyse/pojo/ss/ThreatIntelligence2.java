package com.broadtech.analyse.pojo.ss;

/**
 * @author leo.J
 * @description 威胁情报库POJO
 * @date 2020-08-10 15:35
 */
public class ThreatIntelligence2 {
    private String geo;
    private String type;
    private String value;
    private String score;
    private String category;
    private String timestamp;

    public ThreatIntelligence2(String geo, String type, String value, String score, String category, String timestamp) {
        this.geo = geo;
        this.type = type;
        this.value = value;
        this.score = score;
        this.category = category;
        this.timestamp = timestamp;
    }

    public String getGeo() {
        return geo;
    }

    public void setGeo(String geo) {
        this.geo = geo;
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

    public String getScore() {
        return score;
    }

    public void setScore(String score) {
        this.score = score;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "ThreatIntelligence2{" +
                "geo='" + geo + '\'' +
                ", type='" + type + '\'' +
                ", value='" + value + '\'' +
                ", score='" + score + '\'' +
                ", category='" + category + '\'' +
                ", timestamp='" + timestamp + '\'' +
                '}';
    }
}
