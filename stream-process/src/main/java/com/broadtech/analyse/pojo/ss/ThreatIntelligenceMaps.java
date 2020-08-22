package com.broadtech.analyse.pojo.ss;

import java.util.Map;

/**
 * @author jiangqingsong
 * @description
 * @date 2020-08-07 17:41
 */
public class ThreatIntelligenceMaps {
    public Map<String, ThreatIntelligence> ip4Map;
    public Map<String, ThreatIntelligence> domainMap;
    public Map<String, ThreatIntelligence> urlMap;
    public Map<String, ThreatIntelligence> hashMd5Map;
    public Map<String, ThreatIntelligence> hashSha1Map;
    public Map<String, ThreatIntelligence> hashSha256Map;
    public Map<String, ThreatIntelligence> emailMap;

    public ThreatIntelligenceMaps() {
    }

    public ThreatIntelligenceMaps(Map<String, ThreatIntelligence> ip4Map, Map<String, ThreatIntelligence> domainMap, Map<String, ThreatIntelligence> urlMap, Map<String, ThreatIntelligence> hashMd5Map, Map<String, ThreatIntelligence> hashSha1Map, Map<String, ThreatIntelligence> hashSha256Map, Map<String, ThreatIntelligence> emailMap) {
        this.ip4Map = ip4Map;
        this.domainMap = domainMap;
        this.urlMap = urlMap;
        this.hashMd5Map = hashMd5Map;
        this.hashSha1Map = hashSha1Map;
        this.hashSha256Map = hashSha256Map;
        this.emailMap = emailMap;
    }

    public Map<String, ThreatIntelligence> getIp4Map() {
        return ip4Map;
    }

    public void setIp4Map(Map<String, ThreatIntelligence> ip4Map) {
        this.ip4Map = ip4Map;
    }

    public Map<String, ThreatIntelligence> getDomainMap() {
        return domainMap;
    }

    public void setDomainMap(Map<String, ThreatIntelligence> domainMap) {
        this.domainMap = domainMap;
    }

    public Map<String, ThreatIntelligence> getUrlMap() {
        return urlMap;
    }

    public void setUrlMap(Map<String, ThreatIntelligence> urlMap) {
        this.urlMap = urlMap;
    }

    public Map<String, ThreatIntelligence> getHashMd5Map() {
        return hashMd5Map;
    }

    public void setHashMd5Map(Map<String, ThreatIntelligence> hashMd5Map) {
        this.hashMd5Map = hashMd5Map;
    }

    public Map<String, ThreatIntelligence> getHashSha1Map() {
        return hashSha1Map;
    }

    public void setHashSha1Map(Map<String, ThreatIntelligence> hashSha1Map) {
        this.hashSha1Map = hashSha1Map;
    }

    public Map<String, ThreatIntelligence> getHashSha256Map() {
        return hashSha256Map;
    }

    public void setHashSha256Map(Map<String, ThreatIntelligence> hashSha256Map) {
        this.hashSha256Map = hashSha256Map;
    }

    public Map<String, ThreatIntelligence> getEmailMap() {
        return emailMap;
    }

    public void setEmailMap(Map<String, ThreatIntelligence> emailMap) {
        this.emailMap = emailMap;
    }
}
