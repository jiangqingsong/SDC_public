package com.broadtech.analyse.flink.process.main;

import com.broadtech.analyse.pojo.main.AssetLoadInfo;
import com.broadtech.analyse.pojo.main.CNVDVulnerability;
import com.broadtech.analyse.pojo.main.VulnUnify;
import com.broadtech.analyse.util.TimeUtils;
import com.broadtech.analyse.util.VulnerabilityUtils;
import com.mysql.jdbc.Driver;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;

/**
 * @author leo.J
 * @description
 * @date 2020-09-23 16:33
 */
public class AssetLoadWithVulnFn extends ProcessFunction<AssetLoadInfo, VulnUnify> {
    private Connection connection = null;
    private PreparedStatement ps1 = null;
    private volatile boolean isRunning = true;

    private String jdbcUrl;
    private String userName;
    private String password;

    private Map<String, CNVDVulnerability> valnerabilityMap;
    private Timer timer;

    public AssetLoadWithVulnFn(String jdbcUrl, String userName, String password) {
        this.jdbcUrl = jdbcUrl;
        this.userName = userName;
        this.password = password;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        valnerabilityMap = new HashMap<>();
        //开启一个定时任务，定期更新配置数据
        timer = new Timer(true);
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                try {
                    //getData();
                    valnerabilityMap = VulnerabilityUtils.getVulnData(ps1, connection, jdbcUrl, userName, password);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }, 1000, 1 * 30 * 60 * 1000);
    }


    @Override
    public void processElement(AssetLoadInfo value, Context ctx, Collector<VulnUnify> out) throws Exception {
        String ip = value.getIp();
        String osName = value.getOsName();
        String programName = value.getProgramName();
        String dbName = value.getDbName();
        String messageOrientedMiddlewares = value.getMessageOrientedMiddlewares();
        String softName = value.getSoftName();
        matchVuln(ip,out, osName);
        matchVuln(ip,out, programName);
        matchVuln(ip,out, dbName);
        matchVuln(ip,out, messageOrientedMiddlewares);
        matchVuln(ip,out, softName);
    }
    /**
     * 漏洞匹配
     * @param productInfo 待碰撞字段信息
     */
    public void matchVuln(String ip, Collector<VulnUnify> out, String productInfo) {
        if(productInfo.isEmpty()){
            return;
        }
        Set<String> products = valnerabilityMap.keySet();
        Set<String> matchedVulnerabilitySet = new HashSet<>();
        String productOut = "";
        for (String product : products) {
            if (product.contains(productInfo)) {
                productOut = product;
                break;
            }
        }
        if(!productOut.isEmpty()){
            String number = valnerabilityMap.get(productOut).getNumber();
            VulnUnify vuLnUnify = new VulnUnify(number, "", ip, "", "1");
            out.collect(vuLnUnify);
        }
    }


    /**
     * 获取漏洞数据和label数据
     *
     * @throws Exception
     */
    public void getData() throws Exception {
        DriverManager.registerDriver(new Driver());
        connection = DriverManager.getConnection(jdbcUrl, userName, password);//获取连接
        String sql1 = "select * from brd_cnvd_vulnerability";
        ps1 = connection.prepareStatement(sql1);

        if (isRunning) {
            ResultSet rs1 = ps1.executeQuery();
            while (rs1.next()) {
                String number = rs1.getString("number");
                String cve_number = rs1.getString("cve_number");
                String cve_url = rs1.getString("cve_url");
                String title = rs1.getString("title");
                String serverity = rs1.getString("serverity");
                String product = rs1.getString("product");
                String is_event = rs1.getString("is_event");
                String submittime = rs1.getString("submit_time");
                String opentime = rs1.getString("open_time");
                String reference_link = rs1.getString("reference_link");
                String formalway = rs1.getString("formalway");
                String description = rs1.getString("description");
                String patchname = rs1.getString("patchname");
                String patch_description = rs1.getString("patch_description");
                String update_time = rs1.getString("update_time");

                //String discoverername = rs1.getString("discoverername");
                if ("".equals(product)) {
                    continue;
                }

                CNVDVulnerability vulnerability = new CNVDVulnerability();
                //Vulnerability vulnerability = new Vulnerability();
                vulnerability.setNumber(number);
                vulnerability.setCveNumber(cve_number);
                vulnerability.setCveUrl(cve_url);
                vulnerability.setTitle(title);
                vulnerability.setServerity(serverity);
                vulnerability.setProduct(product);
                vulnerability.setIsEvent(is_event);
                vulnerability.setSubmitTime(submittime);
                vulnerability.setOpenTime(opentime);
                vulnerability.setReferenceLink(reference_link);
                vulnerability.setFormalway(formalway);
                vulnerability.setDescription(description);
                vulnerability.setPatchName(patchname);
                vulnerability.setPatchDescription(patch_description);
                vulnerability.setUpdateTime(update_time);
                valnerabilityMap.put(product, vulnerability);
            }
        }

    }

}
