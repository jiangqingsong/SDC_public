package com.broadtech.analyse.constants.ss;

/**
 * @author jiangqingsong
 * @description
 * @date 2020-08-07 17:14
 */
public enum ThreatIntelligenceType {
    FEED_IPV4("feed_ipv4"),
    FEED_DOMAIN("feed_domain"),
    FEED_URL("feed_url"),
    FEED_HASH_MD5("feed_hash-md5"),
    FEED_HASH_SHA1("feed_hash-sha1"),
    FEED_HASH_SHA256("feed_hash-sha256"),
    FEED_EMAIL("feed_email");
    public String value;
    ThreatIntelligenceType(String value) {
        this.value = value;
    }


    public static void main(String[] args) {
        System.out.println(ThreatIntelligenceType.FEED_IPV4.value);
    }
}
