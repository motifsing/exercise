package com.motifsing.flink.statistics.pv;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.net.URL;
import java.util.Random;

/**
 * @ClassName PageViewStatMain
 * @Description 热门页面
 * @Author Motifsing
 * @Date 2021/3/11 10:21
 * @Version 1.0
 **/
public class PageViewStatMain {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 2. 创建数据源
        URL resource = PageViewStatMain.class.getResource("/UserBehavior.csv");
        DataStreamSource<String> inputStream = env.readTextFile(resource.getPath());

        // 3. 转换为POJO，分配时间戳和watermark
        SingleOutputStreamOperator<UserBehavior> mapStream = inputStream
                .map(line -> {
                    String[] split = line.split(",");
                    if (split.length != 5) {
                        return new UserBehavior();
                    }
                    Long userId = new Long(split[0]);
                    Long itemId = new Long(split[1]);
                    Integer categoryId = new Integer(split[2]);
                    String behavior = split[3];
                    Long timestamp = new Long(split[4]);
                    return new UserBehavior(userId, itemId, categoryId, behavior, timestamp);
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(UserBehavior element) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        // 4. 分组开窗聚合，得到每个窗口的各个商品count值
//        SingleOutputStreamOperator<Tuple2<String, Long>> pvResultStream = mapStream
//                .filter(data -> "pv".equals(data.getBehavior()))
//                .map(data -> Tuple2.of("pv", 1L)).returns(Types.TUPLE(Types.STRING, Types.LONG))
//                .keyBy(0)
//                .timeWindow(Time.hours(1))
//                .sum(1);

        SingleOutputStreamOperator<PageViewCount> aggResultStream = mapStream
                .filter(data -> "pv".equals(data.getBehavior()))
                .map((MapFunction<UserBehavior, Tuple2<Integer, Long>>) value -> {
                    Random random = new Random();
                    return Tuple2.of(random.nextInt(10), 1L);
                }).returns(Types.TUPLE(Types.INT, Types.LONG))
                .keyBy(f -> f.f0)
                .timeWindow(Time.hours(1))
                .aggregate(new PvCountAgg(), new PvCountResult());

        SingleOutputStreamOperator<PageViewCount> pvResultStream = aggResultStream
                .keyBy(PageViewCount::getWindowEnd)
                .process(new TotalPvCount());

        pvResultStream.print();

        env.execute("pv count stat");
    }

    public static class PvCountAgg implements AggregateFunction<Tuple2<Integer, Long>, Long, Long>{

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Tuple2<Integer, Long> value, Long accumulator) {
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

    public static class PvCountResult implements WindowFunction<Long, PageViewCount, Integer, TimeWindow>{

        @Override
        public void apply(Integer integer, TimeWindow window, Iterable<Long> input, Collector<PageViewCount> out) throws Exception {
            out.collect(new PageViewCount(integer.toString(), window.getEnd(), input.iterator().next()));
        }
    }

    public static class TotalPvCount extends KeyedProcessFunction<Long, PageViewCount, PageViewCount>{

        ValueState<Long> totalCountState;

        @Override
        public void open(Configuration parameters) throws Exception {
            totalCountState = getRuntimeContext().getState(new ValueStateDescriptor<>("total-count-state", Types.LONG, 0L));
        }

        @Override
        public void processElement(PageViewCount value, Context ctx, Collector<PageViewCount> out) throws Exception {
            totalCountState.update(totalCountState.value() + value.getCount());
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<PageViewCount> out) throws Exception {
            Long totalCount = totalCountState.value();
            out.collect(new PageViewCount("pv", ctx.getCurrentKey(), totalCount));
            totalCountState.clear();
        }
    }
}
