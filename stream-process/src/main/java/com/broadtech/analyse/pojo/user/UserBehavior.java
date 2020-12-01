package com.broadtech.analyse.pojo.user;

/**
 * @author leo.J
 * @description
 * @date 2020-05-11 15:31
 */
public class UserBehavior {
    private long userId;         // 用户ID
    private long itemId;         // 商品ID
    private int categoryId;      // 商品类目ID
    private String behavior;     // 用户行为, 包括("pv", "buy", "cart", "fav")
    private long timestamp;      // 行为发生的时间戳，单位秒

    public UserBehavior(long userId, long itemId, int categoryId, String behavior, long timestamp) {
        this.userId = userId;
        this.itemId = itemId;
        this.categoryId = categoryId;
        this.behavior = behavior;
        this.timestamp = timestamp;
    }

    public long getUserId() {
        return userId;
    }

    public void setUserId(long userId) {
        this.userId = userId;
    }

    public long getItemId() {
        return itemId;
    }

    public void setItemId(long itemId) {
        this.itemId = itemId;
    }

    public int getCategoryId() {
        return categoryId;
    }

    public void setCategoryId(int categoryId) {
        this.categoryId = categoryId;
    }

    public String getBehavior() {
        return behavior;
    }

    public void setBehavior(String behavior) {
        this.behavior = behavior;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
