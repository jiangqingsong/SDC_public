package com.broadtech.analyse.flink.process;

import com.broadtech.analyse.pojo.gateway.GatewayAppTypeView;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author jiangqingsong
 * @description
 * @date 2020-05-11 16:54
 */
public class AppTypeRateProcessFn extends KeyedProcessFunction<Long, GatewayAppTypeView, String> {

    private ListState<GatewayAppTypeView> appTypeState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ListStateDescriptor<GatewayAppTypeView> appTypeStateDes = new ListStateDescriptor<>("appTypeState", GatewayAppTypeView.class);
        appTypeState = getRuntimeContext().getListState(appTypeStateDes);
    }

    @Override
    public void processElement(GatewayAppTypeView value, Context ctx, Collector<String> out) throws Exception {
        appTypeState.add(value);

    }
}
