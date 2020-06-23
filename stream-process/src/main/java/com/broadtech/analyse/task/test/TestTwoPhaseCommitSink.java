package com.broadtech.analyse.task.test;

import com.broadtech.analyse.flink.sink.MySqlTwoPhaseCommitSinkTest;
import com.broadtech.analyse.util.env.FlinkUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author jiangqingsong
 * @description
 * @date 2020-06-10 19:57
 */
public class TestTwoPhaseCommitSink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = FlinkUtils.getEnv();
        env.fromElements("test").addSink(new MySqlTwoPhaseCommitSinkTest());
        env.execute("test two phase commit sink.");
    }

}
