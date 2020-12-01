package com.broadtech.analyse.pojo.cmcc;

import com.alibaba.fastjson.JSON;

/**
 * @author leo.J
 * @description Agent消息中间件
 * @date 2020-06-09 16:34
 */
public class MessageOrientedMiddleware {
    private String Name;
    private String Version;
    private String Port;
    private String Path;
    private String User;
    private String DevelopmentLanguage;
    private String Plug_in_library_name;


    public String getName() {
        return Name;
    }

    public void setName(String name) {
        Name = name;
    }

    public String getVersion() {
        return Version;
    }

    public void setVersion(String version) {
        Version = version;
    }

    public String getPort() {
        return Port;
    }

    public void setPort(String port) {
        Port = port;
    }

    public String getPath() {
        return Path;
    }

    public void setPath(String path) {
        Path = path;
    }

    public String getUser() {
        return User;
    }

    public void setUser(String user) {
        User = user;
    }

    public String getDevelopmentLanguage() {
        return DevelopmentLanguage;
    }

    public void setDevelopmentLanguage(String developmentLanguage) {
        DevelopmentLanguage = developmentLanguage;
    }

    public String getPlug_in_library_name() {
        return Plug_in_library_name;
    }

    public void setPlug_in_library_name(String plug_in_library_name) {
        this.Plug_in_library_name = plug_in_library_name;
    }

    public static void main(String[] args) {
        String str = "[{\"Name\":\"apache-tomcat\",\"Version\":\" Apache Tomcat/9.0.35\",\"Port\":8005,\"Path\":\"/home/brd/apache-tomcat-9.0.35\",\"User\":\"brd\",\"DevelopmentLanguage\":\"java\",\"Plug_in_library_name\":[\"core_module\",\"so_module\",\"http_module\",\"access_compat_module\",\"actions_module\",\"alias_module\",\"allowmethods_module\",\"auth_basic_module\",\"auth_digest_module\",\"authn_anon_module\",\"authn_core_module\",\"authn_dbd_module\",\"authn_dbm_module\",\"authn_file_module\",\"authn_socache_module\",\"authz_core_module\",\"authz_dbd_module\",\"authz_dbm_module\",\"authz_groupfile_module\",\"authz_host_module\",\"authz_owner_module\",\"authz_user_module\",\"autoindex_module\",\"cache_module\",\"cache_disk_module\",\"data_module\",\"dbd_module\",\"deflate_module\",\"dir_module\",\"dumpio_module\",\"echo_module\",\"env_module\",\"expires_module\",\"ext_filter_module\",\"filter_module\",\"headers_module\",\"include_module\",\"info_module\",\"log_config_module\",\"logio_module\",\"mime_magic_module\",\"mime_module\",\"negotiation_module\",\"remoteip_module\",\"reqtimeout_module\",\"rewrite_module\",\"setenvif_module\",\"slotmem_plain_module\",\"slotmem_shm_module\",\"socache_dbm_module\",\"socache_memcache_module\",\"socache_shmcb_module\",\"status_module\",\"substitute_module\",\"suexec_module\",\"unique_id_module\",\"unixd_module\",\"userdir_module\",\"version_module\",\"vhost_alias_module\",\"dav_module\",\"dav_fs_module\",\"dav_lock_module\",\"lua_module\",\"mpm_prefork_module\",\"proxy_module\",\"lbmethod_bybusyness_module\",\"lbmethod_byrequests_module\",\"lbmethod_bytraffic_module\",\"lbmethod_heartbeat_module\",\"proxy_ajp_module\",\"proxy_balancer_module\",\"proxy_connect_module\",\"proxy_express_module\",\"proxy_fcgi_module\",\"proxy_fdpass_module\",\"proxy_ftp_module\",\"proxy_http_module\",\"proxy_scgi_module\",\"proxy_wstunnel_module\",\"ssl_module\",\"systemd_module\",\"cgi_module\"]}]";

        java.util.List<MessageOrientedMiddleware> messageOrientedMiddlewares = JSON.parseArray(str, MessageOrientedMiddleware.class);
        /*TypeUtils.compatibleWithFieldName = true;
        TypeUtils.compatibleWithJavaBean=true;*/
        System.out.println(messageOrientedMiddlewares.size());


    }

}
