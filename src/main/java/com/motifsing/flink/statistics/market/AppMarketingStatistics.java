package com.motifsing.flink.statistics.market;

import com.motifsing.flink.statistics.market.beans.ChannelPromotionCount;
import com.motifsing.flink.statistics.market.beans.MarketingUserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * @ClassName AppMarketingStatistics
 * @Description
 * @Author Motifsing
 * @Date 2021/3/30 10:35
 * @Version 1.0
 **/
public class AppMarketingStatistics {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 1. 从自定义数据源中读取数据
        DataStream<MarketingUserBehavior> dataStream = env.addSource(new AppMarketingByChannel.SimulateMarketingUserBehaviorSource())
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<MarketingUserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(MarketingUserBehavior element) {
                        return element.getTimestamp();
                    }
                });

        // 2. 开窗统计
        SingleOutputStreamOperator<ChannelPromotionCount> resultStream = dataStream
                .filter(data -> !"UNINSTALL".equals(data.getBehavior()))
                .map(new MapFunction<MarketingUserBehavior, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(MarketingUserBehavior value) throws Exception {
                        return Tuple2.of("total", 1L);
                    }
                })
                .keyBy(0)
                .timeWindow(Time.hours(1), Time.seconds(5))
                .aggregate(new MarketingStatisticsAgg(), new MarketingStatisticsResult());

        resultStream.print();

        env.execute("app marketing statistics job");
    }

    public static class MarketingStatisticsAgg implements AggregateFunction<Tuple2<String, Long>, Long, Long>{

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Tuple2<String, Long> value, Long accumulator) {
            return accumulator + value.f1;
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

    public static class MarketingStatisticsResult extends ProcessWindowFunction<Long, ChannelPromotionCount, Tuple, TimeWindow>{

        @Override
        public void process(Tuple s, Context context, Iterable<Long> elements, Collector<ChannelPromotionCount> out) throws Exception {
            Long count = elements.iterator().next();
            String windowEnd = new Timestamp(context.window().getEnd()).toString();

            out.collect(new ChannelPromotionCount(s.getField(0), "total", windowEnd, count));
        }
    }
}
