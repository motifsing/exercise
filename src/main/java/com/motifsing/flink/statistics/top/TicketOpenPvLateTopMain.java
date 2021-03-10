package com.motifsing.flink.statistics.top;

import com.clearspring.analytics.util.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.jetbrains.annotations.NotNull;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @ClassName TicketOpenPvTopNMain
 * @Description 统计最近1分钟发券量前五的礼券id，10s更新一次
 * @Author Motifsing
 * @Date 2021/3/4 16:33
 * @Version 1.0
 **/
public class TicketOpenPvLateTopMain {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        OutputTag<Tuple2<String, Long>> lateTag = new OutputTag<Tuple2<String, Long>>("late") {};

        // 创建数据源
        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);
        SingleOutputStreamOperator<Tuple2<String, Long>> output = source
                .map(new SourceMap())
                .assignTimestampsAndWatermarks(new PeriodicWatermarks());

        SingleOutputStreamOperator<Tuple3<String, Long, Long>> aggregate = output
                .keyBy(f -> f.f0)
                .window(SlidingEventTimeWindows.of(Time.seconds(60), Time.seconds(5)))
                .allowedLateness(Time.seconds(3))
                .sideOutputLateData(lateTag)
                .aggregate(new AggStatistics(), new WindowProcess());

        aggregate.print("aggregate:");

        aggregate
                .keyBy(f -> f.f1)
                .process(new KeyedProcess())
                .print();

        aggregate
                .getSideOutput(lateTag)
                .print("late:");

        env.execute("activityTopCount");
    }

    public static class SourceMap implements MapFunction<String, Tuple2<String, Long>> {

        @Override
        public Tuple2<String, Long> map(String value) throws Exception {
            String[] s = value.split("\\|");
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
            return Tuple2.of(s[0], sdf.parse(s[1]).getTime());
        }
    }

    public static class PeriodicWatermarks implements AssignerWithPeriodicWatermarks<Tuple2<String, Long>>{

        private long currentMaxTimestamp = 0L;

        @NotNull
        @Override
        public Watermark getCurrentWatermark() {
            // 允许最大乱序时间
            long maxOutOfOrderTime = 1000L;
            return new Watermark(currentMaxTimestamp - maxOutOfOrderTime);
        }

        @Override
        public long extractTimestamp(Tuple2<String, Long> element, long previousElementTimestamp) {
            long eventTimestamp = element.f1;
            currentMaxTimestamp = Math.max(eventTimestamp, currentMaxTimestamp);
            return eventTimestamp;
        }
    }

    public static class AggStatistics implements AggregateFunction<Tuple2<String, Long>, Long, Long>{
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Tuple2<String, Long> value, Long accumulator) {
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

    public static class WindowProcess implements WindowFunction<Long, Tuple3<String, Long, Long>, String, TimeWindow>{
        @Override
        public void apply(String s, TimeWindow window, Iterable<Long> input, Collector<Tuple3<String, Long, Long>> out) throws Exception {
            out.collect(Tuple3.of(s, window.getEnd(), input.iterator().next()));
        }
    }

    public static class KeyedProcess extends KeyedProcessFunction<Long, Tuple3<String, Long, Long>, String> {
        MapState<Long, Long> activityMapState;

        @Override
        public void open(Configuration parameters) throws Exception{
            activityMapState = getRuntimeContext().getMapState(new MapStateDescriptor<Long, Long>("activityMapState", Long.class, Long.class));
        }

        @Override
        public void processElement(Tuple3<String, Long, Long> value, Context ctx, Collector<String> out) throws Exception {
            activityMapState.put(value.f1, value.f2);
            ctx.timerService().registerEventTimeTimer(value.f1 + 1000L);
            // 注册一个3s后的定时器，用来清空状态
            ctx.timerService().registerEventTimeTimer(value.f1 + 3000L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 先判断是否到了窗口关闭清理时间，如果是，直接清空状态返回
            if (timestamp == ctx.getCurrentKey() + 3000L){
                activityMapState.clear();
                return;
            }


            List<Map.Entry<Long, Long>> entries = Lists.newArrayList(activityMapState.entries());

            entries.sort(new Comparator<Map.Entry<Long, Long>>() {
                @Override
                public int compare(Map.Entry<Long, Long> o1, Map.Entry<Long, Long> o2) {
                    return o2.getValue().compareTo(o1.getValue());
                }
            });

            StringBuilder sb = new StringBuilder();
            sb.append("=======================================\n");
            String format = "yyyy/MM/dd  HH:mm:ss";
            SimpleDateFormat sdf = new SimpleDateFormat(format);
            String dateTimeStr = sdf.format(timestamp - 1000);
            sb.append("窗口结束时间：").append(dateTimeStr).append("\n");

            for (int i=0; i<Math.min(3, entries.size());i++){
                Map.Entry<Long, Long> longLongEntry = entries.get(i);
                sb.append("NO").append(i+1).append(": ")
                        .append("ID -> ").append(longLongEntry.getKey())
                        .append(", 热门度 -> ").append(longLongEntry.getValue()).append("\n");
            }
            sb.append("=========================================\n\n");

            Thread.sleep(1000);

            out.collect(sb.toString());

        }
    }
}
