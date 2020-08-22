package com.broadtech.analyse.task.abnormal;

import com.broadtech.analyse.flink.deserialization.CustomJSONDeserializationSchema;
import com.broadtech.analyse.flink.function.abnormal.DpiJson2Obj;
import com.broadtech.analyse.flink.function.abnormal.FilterCCIp;
import com.broadtech.analyse.flink.process.abnormal.FilterJsonProcess;
import com.broadtech.analyse.flink.process.abnormal.IntranetFallProcess;
import com.broadtech.analyse.flink.process.abnormal.KeywordMatchProcess;
import com.broadtech.analyse.flink.sink.abnormal.AlarmResultSink;
import com.broadtech.analyse.flink.watermark.abnormal.DpiAssignTimestampAndWatermarks;
import com.broadtech.analyse.flink.window.abnormal.CcAnalysisWindowFunc;
import com.broadtech.analyse.flink.window.abnormal.CcAnalysisWindowFunc2;
import com.broadtech.analyse.flink.window.abnormal.ResultWindowFunc;
import com.broadtech.analyse.flink.window.abnormal.ScanAnalysisWindowFunc;
import com.broadtech.analyse.pojo.abnormal.AlarmResult;
import com.broadtech.analyse.pojo.abnormal.Dpi;
import com.broadtech.analyse.util.env.FlinkUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import org.apache.log4j.Logger;

import java.util.List;

/**
 * @author jiangqingsong
 * @description DPI流量分析
 * @date 2020-08-21 10:01
 */
public class DpiAnalysis {
    private static Logger LOG = Logger.getLogger(DpiAnalysis.class);
    private static final OutputTag<ObjectNode> jsonFormatTag = new OutputTag<ObjectNode>("JsonFormat") {
    };
    private static final String TIME_PATTERN = "yyyy-MM-dd HH:mm:ss";
    public static void main(String[] args) throws Exception {

        String propPath = "D:\\SDC\\gitlab_code\\sdcplatform\\SDCPlatform\\stream-process\\src\\main\\resources\\telecom_dpi_analysis.properties";
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        //String propPath = parameterTool.get("conf_path");
        //获取配置参数
        ParameterTool paramFromProps = ParameterTool.fromPropertiesFile(propPath);
        String consumerTopic = paramFromProps.get("consumer.topic");
        String producerBrokers = paramFromProps.get("producer.bootstrap.server");
        String groupId = paramFromProps.get("consumer.groupId");

        Long windowSize = paramFromProps.getLong("windowSize");
        Long batchSize = paramFromProps.getLong("batchSize");

        //sink mysql config
        String jdbcUrl = paramFromProps.get("jdbcUrl");
        String userName = paramFromProps.get("userName");
        String password = paramFromProps.get("password");

        //黑白名单
        String blackList = paramFromProps.get("blackList");
        String whiteList = paramFromProps.get("whiteList");
        String keyword = paramFromProps.get("keyword");
        //配置阈值
        Integer abnormalTimes = paramFromProps.getInt("abnormalTimes");
        Integer abnormalTrafficSize = paramFromProps.getInt("abnormalTrafficSize");
        Integer srcIpCount = paramFromProps.getInt("srcIpCount");
        Integer portCount = paramFromProps.getInt("portCount");
        Double trafficUpperLimit = paramFromProps.getDouble("trafficUpperLimit");

        //env
        final StreamExecutionEnvironment env = FlinkUtils.getEnv();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        DataStream<ObjectNode> dpiSourceStream = FlinkUtils.createKafkaStream(true, paramFromProps, consumerTopic, groupId, CustomJSONDeserializationSchema.class);
        DataStream<ObjectNode> filteredDpiSourceStream = dpiSourceStream.process(new FilterJsonProcess()).getSideOutput(jsonFormatTag);

        //分配时间戳
        SingleOutputStreamOperator<Dpi> dpiObjStream = filteredDpiSourceStream.map(new DpiJson2Obj())
                .assignTimestampsAndWatermarks(new DpiAssignTimestampAndWatermarks(Time.seconds(5)));

        //1、C&C 分析
        SingleOutputStreamOperator<AlarmResult> ccAbnormalStream = dpiObjStream.filter(new FilterCCIp(blackList, whiteList))
                .keyBy(dpi -> Tuple2.of(dpi.getSrcIPAddress(), dpi.getDestIPAddress()))
                .timeWindow(Time.minutes(windowSize))
                .process(new CcAnalysisWindowFunc2(abnormalTimes, abnormalTrafficSize));
        SingleOutputStreamOperator<List<AlarmResult>> ccAbnormalBatchStream = ccAbnormalStream.countWindowAll(batchSize).apply(new ResultWindowFunc());

        SingleOutputStreamOperator<AlarmResult> ccAbnormal2Stream = dpiObjStream.filter(new FilterCCIp(blackList, whiteList))
                .keyBy(dpi -> dpi.getDestIPAddress())
                .timeWindow(Time.minutes(windowSize))
                .process(new CcAnalysisWindowFunc(srcIpCount));
        SingleOutputStreamOperator<List<AlarmResult>> ccAbnormal2BatchStream = ccAbnormal2Stream.countWindowAll(batchSize).apply(new ResultWindowFunc());

        //2、可以数据外传
        SingleOutputStreamOperator<AlarmResult> suspiciousOutStream = dpiObjStream.process(new KeywordMatchProcess(jdbcUrl, userName, password, keyword));
        SingleOutputStreamOperator<List<AlarmResult>> suspiciousOutBatchStream = suspiciousOutStream.countWindowAll(batchSize).apply(new ResultWindowFunc());

        //3、扫描探测行为分析
        SingleOutputStreamOperator<AlarmResult> scanStream = dpiObjStream.keyBy(dpi -> dpi.getDestIPAddress())
                .timeWindow(Time.minutes(windowSize))
                .process(new ScanAnalysisWindowFunc(portCount));
        SingleOutputStreamOperator<List<AlarmResult>> scanBatchStream = scanStream.countWindowAll(batchSize).apply(new ResultWindowFunc());

        //4.内网失陷主机检测
        SingleOutputStreamOperator<AlarmResult> intranetFallStream = dpiObjStream.process(new IntranetFallProcess(trafficUpperLimit));
        SingleOutputStreamOperator<List<AlarmResult>> intranetFallBatchStream = intranetFallStream.countWindowAll(batchSize).apply(new ResultWindowFunc());

        //sink
        ccAbnormalBatchStream.addSink(new AlarmResultSink(jdbcUrl, userName, password));
        ccAbnormal2BatchStream.addSink(new AlarmResultSink(jdbcUrl, userName, password));
        suspiciousOutBatchStream.addSink(new AlarmResultSink(jdbcUrl, userName, password));
        scanBatchStream.addSink(new AlarmResultSink(jdbcUrl, userName, password));
        intranetFallBatchStream.addSink(new AlarmResultSink(jdbcUrl, userName, password));

        env.execute("Telecom Abnormal Analysis Job.");
    }
}
