package com.broadtech.analyse.flink.function.cmcc;

import com.alibaba.fastjson.JSON;
import com.broadtech.analyse.pojo.cmcc.*;
import com.mysql.jdbc.Driver;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;

/**
 * @author leo.J
 * @description
 * @date 2020-06-11 17:15
 */
public class AgentWithLabelProcessFun extends ProcessFunction<AssetAgentOrigin, Tuple2<AssetAgentOrigin, Tuple3<String, String, String>>> {
    private Connection connection2 = null;
    private PreparedStatement ps1 = null;
    private PreparedStatement ps2 = null;
    private BasicDataSource dataSource = null;
    private volatile boolean isRunning = true;

    private String jdbcUrl;
    private String userName;
    private String password;

    private Map<String, Tuple3<Integer, String, String>> labelMap;
    private Timer timer;
    public AgentWithLabelProcessFun(String jdbcUrl, String userName, String password) {
        this.jdbcUrl = jdbcUrl;
        this.userName = userName;
        this.password = password;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        labelMap = new HashMap<>();
        //开启一个定时任务，定期更新配置数据
        timer = new Timer(true);
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                try {
                    getData();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }, 1000, 1*30*60*1000);
    }

    @Override
    public void close() throws Exception {
        super.close();
        timer.cancel();
        timer = null;
    }

    /**
     * 获取漏洞数据和label数据
     * @throws Exception
     */
    public void getData() throws Exception{
        DriverManager.registerDriver(new Driver());
        connection2 = DriverManager.getConnection(jdbcUrl, userName, password);
        String sql2 = "select l.id as id,l.label_1 as label1,l.label_2 as label2,k.keyword as keyword from label l join label_key k on l.id = k.label_id";
        ps2 = connection2.prepareStatement(sql2);

        if (isRunning) {
            ResultSet rs2 = ps2.executeQuery();
            while (rs2.next()) {
                int id = rs2.getInt("id");
                String label1 = rs2.getString("label1");
                String label2 = rs2.getString("label2");
                String keyword = rs2.getString("keyword");
                labelMap.put(keyword, Tuple3.of(id, label1, label2));
            }
        }

    }

    @Override
    public void processElement(AssetAgentOrigin agent, Context context, Collector<Tuple2<AssetAgentOrigin, Tuple3<String, String, String>>> out) throws Exception {
        if(labelMap != null){
            //待检测漏洞的Tuple<name, version>列表
            List<Tuple2<String, String>> toCheckList = new ArrayList<>();
            List<ProgramInfo> programInfos = agent.getProgramInfos();
            List<MessageOrientedMiddleware> messageOrientedMiddlewares = agent.getMessageOrientedMiddlewares();
            List<DataBaseInfo> dataBaseInfos = agent.getDataBaseInfos();
            for(ProgramInfo p: programInfos){
                toCheckList.add(Tuple2.of(p.getName(), p.getVersion()));
            }
            for(MessageOrientedMiddleware m: messageOrientedMiddlewares){
                toCheckList.add(Tuple2.of(m.getName(), m.getVersion()));
            }
            for(DataBaseInfo d: dataBaseInfos){
                toCheckList.add(Tuple2.of(d.getName(), d.getVersion()));
            }

            String labelInputInfo = StringUtils.join(toCheckList, ",")
                    + agent.getDeviceName() + "," + agent.getDeviceType()
                    + "," + agent.getOSInfo();
            Set<Tuple3<Integer, String, String>> matchedLabels = matchLabel(labelInputInfo);
            long t1 = System.currentTimeMillis();

            if(matchedLabels.size() == 0){
                out.collect(Tuple2.of(agent, Tuple3.of("", "", "")));
            }else {
                String separator = ",";
                List<String> labelIds = new ArrayList<>();
                List<String> type1List = new ArrayList<>();
                List<String> type2List = new ArrayList<>();
                for(Tuple3<Integer, String, String> label: matchedLabels){
                    labelIds.add(String.valueOf(label.f0));
                    type1List.add(label.f1);
                    type2List.add(label.f2);
                }

                out.collect(Tuple2.of(agent, Tuple3.of(StringUtils.join(labelIds, separator), StringUtils.join(type1List, separator), StringUtils.join(type2List, separator))));
            }

            final OutputTag<String> completedStatusTag = new OutputTag<String>("completedStatusTag") {
            };
            context.output(completedStatusTag, agent.getTaskID());
        }
    }

    /**
     *
     * @param input 指纹信息
     * @return labels
     */
    public Set<Tuple3<Integer, String, String>> matchLabel(String input){
        Set<Tuple3<Integer, String, String>> labels = new HashSet<>();
        Set<String> keywords = labelMap.keySet();
        for(String keyword: keywords){
            if(input.toLowerCase().contains(keyword.toLowerCase())){
                labels.add(labelMap.get(keyword));
            }
        }
        return labels;
    }
}
