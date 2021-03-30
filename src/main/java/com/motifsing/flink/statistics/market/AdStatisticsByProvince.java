package com.motifsing.flink.statistics.market;

import com.motifsing.flink.statistics.market.beans.AdClickEvent;
import com.motifsing.flink.statistics.market.beans.AdCountViewByProvince;
import com.motifsing.flink.statistics.market.beans.BlackListUserWarning;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.net.URL;
import java.sql.Timestamp;

/**
 * @ClassName AdStatisticsByProvince
 * @Description
 * @Author Motifsing
 * @Date 2021/3/30 12:53
 * @Version 1.0
 **/
public class AdStatisticsByProvince {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        URL resource = AdStatisticsByProvince.class.getResource("/AdClickLog.csv");
        SingleOutputStreamOperator<AdClickEvent> adClickEventStream = env.readTextFile(resource.getPath())
                .map(line -> {
                    String[] fields = line.split(",");
                    return new AdClickEvent(new Long(fields[0]), new Long(fields[1]), fields[2], fields[3], new Long(fields[4]));
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<AdClickEvent>() {
                    @Override
                    public long extractAscendingTimestamp(AdClickEvent element) {
                        return element.getTimestamp() * 1000;
                    }
                });

        // 对同一个用户点击同一个广告进行检测报警
        SingleOutputStreamOperator<AdClickEvent> filterAdClickStream = adClickEventStream
                .keyBy("userId", "adId")
                .process(new FilterBlackListUser(100));

        // 基于省份分组，开窗聚合
        SingleOutputStreamOperator<AdCountViewByProvince> adCountResultStream = filterAdClickStream
                .keyBy(AdClickEvent::getProvince)
                .timeWindow(Time.hours(1), Time.minutes(5))
                .aggregate(new AdCountAgg(), new AdCountResult());

        adCountResultStream.print();

        filterAdClickStream.getSideOutput(OT).print("blacklist-user");

        env.execute("ad count by province job");
    }

    public static final OutputTag<BlackListUserWarning> OT = new OutputTag<BlackListUserWarning>("blackList"){};

    public static class AdCountAgg implements AggregateFunction<AdClickEvent, Long, Long>{

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(AdClickEvent value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    public static class AdCountResult implements WindowFunction<Long, AdCountViewByProvince, String, TimeWindow> {

        @Override
        public void apply(String province, TimeWindow window, Iterable<Long> input, Collector<AdCountViewByProvince> out) throws Exception {
            String windowEnd = new Timestamp(window.getEnd()).toString();
            Long count = input.iterator().next();
            out.collect(new AdCountViewByProvince(province, windowEnd, count));
        }
    }

    public static class FilterBlackListUser extends KeyedProcessFunction<Tuple, AdClickEvent, AdClickEvent>{

        // 定义点击次数上限
        private Integer countUpperBound;

        public FilterBlackListUser(Integer countUpperBound) {
            this.countUpperBound = countUpperBound;
        }

        // 定义状态，保存当前用户对某一个广告的点击次数
        ValueState<Long> countState;

        // 定义一个状态，保存当前用户是否被发送到黑名单当中
        ValueState<Boolean> isSentState;

        @Override
        public void open(Configuration parameters) throws Exception {
            countState = getRuntimeContext().getState(new ValueStateDescriptor<>("ad-count", Long.class));
            isSentState = getRuntimeContext().getState(new ValueStateDescriptor<>("is-sent", Boolean.class));
        }

        @Override
        public void processElement(AdClickEvent value, Context ctx, Collector<AdClickEvent> out) throws Exception {
            // 判断当前用户对同一个广告的点击次数，如果没有达到上限，就count+1正常输出；否则，就过滤掉，并测输出流输出到黑名单
            Long curCount = countState.value();
            if (curCount == null) {
                curCount = 0L;
            }

            // 1. 判断是否是第一个数据，如果是，注册一个第二天0点的定时器
            if (curCount == 0){
                long ts = (ctx.timerService().currentProcessingTime() / (24 * 60 * 60 * 1000) + 1) * (24 * 60 * 60 * 1000) - (8 * 60 * 60 * 1000);
                ctx.timerService().registerProcessingTimeTimer(ts);
            }

            // 2. 判断是否报警
            if (curCount >= countUpperBound){
                // 判断是否输出到黑名单，如果没有，就输出到测输出流
                if (isSentState.value() == null) {
                    isSentState.update(true);
                    ctx.output(OT, new BlackListUserWarning(value.getUserId(), value.getAdId(), "click over " + countUpperBound + " times"));
                }
                return;
            }

            // 如果没有达到上限，count+1
            countState.update(curCount + 1);
            out.collect(value);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<AdClickEvent> out) throws Exception {
            countState.clear();
            isSentState.clear();
        }
    }
}
