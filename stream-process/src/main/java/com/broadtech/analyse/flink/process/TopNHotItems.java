package com.broadtech.analyse.flink.process;

import com.broadtech.analyse.pojo.user.ItemViewCount;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;

/**
 * @author leo.J
 * @description
 * @date 2020-05-11 16:00
 */
public class TopNHotItems extends KeyedProcessFunction<Long, ItemViewCount, String> {
    private int topN;

    public TopNHotItems(int topN) {
        this.topN = topN;
    }

    private ListState<ItemViewCount> itemState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ListStateDescriptor<ItemViewCount> itemStateDescriptor
                = new ListStateDescriptor<>("", ItemViewCount.class);
        itemState = getRuntimeContext().getListState(itemStateDescriptor);
    }

    @Override
    public void processElement(ItemViewCount value, Context ctx, Collector<String> out) throws Exception {
        long windowEnd = value.getWindowEnd();
        itemState.add(value);
        ctx.timerService().registerEventTimeTimer(windowEnd + 1);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        Iterable<ItemViewCount> itemViewCounts = itemState.get();
        ArrayList<ItemViewCount> allItems = new ArrayList<>();
        for(ItemViewCount item: itemViewCounts){
            allItems.add(item);
        }

        allItems.sort(new Comparator<ItemViewCount>() {
            @Override
            public int compare(ItemViewCount o1, ItemViewCount o2) {
                return (int)(o2.getViewCount() - o1.getViewCount());
            }
        });
        StringBuilder result = new StringBuilder();
        result.append("====================================\n");
        result.append("时间: ").append(new Timestamp(timestamp-1)).append("\n");
        for (int i=0;i<allItems.size();i++) {
            ItemViewCount currentItem = allItems.get(i);
            // No1:  商品ID=12224  浏览量=2413
            result.append("No").append(i).append(":")
                    .append("  商品ID=").append(currentItem.getItemId())
                    .append("  浏览量=").append(currentItem.getViewCount())
                    .append("\n");
        }
        result.append("====================================\n\n");
        out.collect(result.toString());
    }
}
