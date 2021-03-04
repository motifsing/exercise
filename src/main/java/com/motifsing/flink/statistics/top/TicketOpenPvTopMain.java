package com.motifsing.flink.statistics.top;

import com.clearspring.analytics.util.Lists;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
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
import org.jetbrains.annotations.NotNull;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 * @ClassName TicketOpenPvTopNMain
 * @Description 统计最近1分钟发券量前五的礼券id，10s更新一次
 * @Author Motifsing
 * @Date 2021/3/4 16:33
 * @Version 1.0
 **/
public class TicketOpenPvTopMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 创建数据源
        DataStreamSource<Tuple3<ActivityEnum, String, Long>> source = env.addSource(new ActivitySourceFunction());

        SingleOutputStreamOperator<Tuple3<String, String, Long>> output = source.map(new MapFunction<Tuple3<ActivityEnum, String, Long>, Tuple3<String, String, Long>>() {
            @Override
            public Tuple3<String, String, Long> map(Tuple3<ActivityEnum, String, Long> value) throws Exception {
                return Tuple3.of(value.f0.getName(), value.f1, value.f2);
            }
        }).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple3<String, String, Long>>() {

            private long currentMaxTimestamp = 0L;
            // 允许最大乱序时间
            private long maxOutOfOrderTime = 10000L;

            @NotNull
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(currentMaxTimestamp - maxOutOfOrderTime);
            }

            @Override
            public long extractTimestamp(Tuple3<String, String, Long> element, long previousElementTimestamp) {
                Long eventTimestamp = element.f2;
                currentMaxTimestamp = Math.max(eventTimestamp, currentMaxTimestamp);
                return eventTimestamp;
            }
        });

        output.keyBy(f -> f.f0)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new AggregateFunction<Tuple3<String, String, Long>, Tuple2<String, Long>, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> createAccumulator() {
                        return Tuple2.of("", 0L);
                    }

                    @Override
                    public Tuple2<String, Long> add(Tuple3<String, String, Long> value, Tuple2<String, Long> accumulator) {
                        return Tuple2.of(accumulator.f0, accumulator.f1 + 1);
                    }

                    @Override
                    public Tuple2<String, Long> getResult(Tuple2<String, Long> accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Tuple2<String, Long> merge(Tuple2<String, Long> a, Tuple2<String, Long> b) {
                        return Tuple2.of(a.f0, a.f1 + b.f1);
                    }
                }, new WindowFunction<Tuple2<String, Long>, Tuple3<String, Long, Long>, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<Tuple2<String, Long>> input, Collector<Tuple3<String, Long, Long>> out) throws Exception {
                        out.collect(Tuple3.of(s, window.getEnd(), input.iterator().next().f1));
                    }
                })
                .keyBy(f -> f.f1)
                .process(new KeyedProcessFunction<Long, Tuple3<String, Long, Long>, String>() {

                    ListState<Tuple3<String, Long, Long>> activityState;

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        Iterator<Tuple3<String, Long, Long>> iterator = activityState.get().iterator();
                        List<Tuple3<String, Long, Long>> copy = new ArrayList<>();
                        while (iterator.hasNext()) {
                            copy.add(iterator.next());
                        }
                        copy.sort(new Comparator<Tuple3<String, Long, Long>>() {
                            @Override
                            public int compare(Tuple3<String, Long, Long> o1, Tuple3<String, Long, Long> o2) {
                                return (int)(o2.f2 - o1.f2);
                            }
                        });

                        StringBuffer sb = new StringBuffer();
                        sb.append("=======================================\n");
                        String format = "yyyy-MM-dd  HH:mm:ss";
                        SimpleDateFormat sdf = new SimpleDateFormat(format);
                        String dateTimeStr = sdf.format(timestamp - 1000);
                        sb.append("窗口结束时间：").append(dateTimeStr).append("\n");

                        for (int i=0; i<Math.min(3, copy.size());i++){
                            Tuple3<String, Long, Long> t = copy.get(i);
                            sb.append("NO").append(i+1).append(": ")
                                    .append("activity_id -> ").append(t.f0)
                                    .append(", 热门度 -> ").append(t.f2).append("\n");
                        }
                        sb.append("=========================================\n\n");

                        Thread.sleep(1000);

                        out.collect(sb.toString());

                    }

                    @Override
                    public void open(Configuration parameters) throws Exception{
                        activityState = getRuntimeContext().getListState(new ListStateDescriptor<>("activityCountState", Types.TUPLE(Types.STRING, Types.LONG, Types.LONG)));
                    }

                    @Override
                    public void processElement(Tuple3<String, Long, Long> value, Context ctx, Collector<String> out) throws Exception {
                        activityState.add(value);
                        ctx.timerService().registerEventTimeTimer(value.f1 + 1000);
                    }
                }).print();

        env.execute("activityTopCount");
    }
}
