package com.broadtech.analyse.pojo.cmcc;

/**
 * @author leo.J
 * @description 资产标签信息
 * @date 2020-06-11 16:14
 */
public class LabelInfo {
    private int id;
    private String label1;
    private String label2;
    private String keyword;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getLabel1() {
        return label1;
    }

    public void setLabel1(String label1) {
        this.label1 = label1;
    }

    public String getLabel2() {
        return label2;
    }

    public void setLabel2(String label2) {
        this.label2 = label2;
    }

    public String getKeyword() {
        return keyword;
    }

    public void setKeyword(String keyword) {
        this.keyword = keyword;
    }
}
