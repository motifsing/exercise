package com.motifsing.flink.statistics.uv;

import com.motifsing.flink.statistics.pv.PageViewCount;
import com.motifsing.flink.statistics.pv.UserBehavior;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisShardInfo;

import java.net.URL;

/**
 * @ClassName UvWithBloomFilter
 * @Description
 * @Author Motifsing
 * @Date 2021/3/29 20:10
 * @Version 1.0
 **/
public class UvWithBloomFilter {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 2. 创建数据源
        URL resource = UniqueVisitor.class.getResource("/UserBehavior.csv");
        DataStreamSource<String> inputStream = env.readTextFile(resource.getPath());

        // 3. 转换为POJO，分配时间戳和watermark
        SingleOutputStreamOperator<UserBehavior> dataStream = inputStream
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

        // 开窗统计
        SingleOutputStreamOperator<PageViewCount> uvStream = dataStream.filter(data -> "pv".equals(data.getBehavior()))
                .timeWindowAll(Time.hours(1))
                .trigger(new MyTrigger())
                .process(new UvCountResultWithBloomFilter());

        uvStream.print();

        env.execute("uv count with bloom filter job");
    }

    /**
     * 实现自定义触发函数
     */

    public static class MyTrigger extends Trigger<UserBehavior, TimeWindow>{

        @Override
        public TriggerResult onElement(UserBehavior element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            // 每来一条数据，直接计算，然后清空窗口
            return TriggerResult.FIRE_AND_PURGE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {

        }
    }

    /**
     * 自定义一个布隆过滤器
     */
    public static class MyBloomFilter {

        // 定义位图大小，一般为2的整次幂
        private Integer cap;

        public MyBloomFilter(Integer cap) {
            this.cap = cap;
        }

        // 实现一个hash函数
        public Long hashCode(String value, Integer seed) {
            long result = 0L;
            for (int i = 0; i < value.length(); i++) {
                result = result * seed + value.charAt(i);
            }
            return result & (cap - 1);
        }
    }

    /**
     * 自定义一个process函数
     */
    public static class UvCountResultWithBloomFilter extends ProcessAllWindowFunction<UserBehavior, PageViewCount, TimeWindow>{

        // 定义Jedis和BloomFilter
        Jedis jedis;
        MyBloomFilter myBloomFilter;

        @Override
        public void open(Configuration parameters) throws Exception {
            jedis = new Jedis("localhost", 6379);
            jedis.auth("123456");
            // 要处理1亿个数据，用64MB大小的位图
            myBloomFilter = new MyBloomFilter(1<<29);
        }

        @Override
        public void process(Context context, Iterable<UserBehavior> elements, Collector<PageViewCount> out) throws Exception {
            // 将位图和窗口count值全部写入redis，用WindowEnd作为key
            Long windowEnd = context.window().getEnd();
            String bitmapKey = windowEnd.toString();
            // 把count值存成一张hash表
            String countHashName = "uv_count";
            String countKey = windowEnd.toString();

            // 1. 取当前的userId
            Long userId = elements.iterator().next().getUserId();

            // 2. 计算位图中的offset
            Long offset = myBloomFilter.hashCode(userId.toString(), 61);

            // 3. 用redis的getbit命令，判断对应位置的值
            Boolean isExist = jedis.getbit(bitmapKey, offset);

            if (!isExist) {
                // 如果不存在，对应位置置1
                jedis.setbit(bitmapKey, offset, true);

                // 更新redis中保存的count值
                // 初始count值
                long uvCount = 0L;
                String uvCountString = jedis.hget(countHashName, countKey);
                if (uvCountString != null && !"".equals(uvCountString)){
                    uvCount = Long.parseLong(uvCountString);
                }
                jedis.hset(countHashName, countKey, String.valueOf(uvCount + 1));
                out.collect(new PageViewCount("uv", windowEnd, uvCount + 1));
            }
        }

        @Override
        public void close() throws Exception {
            jedis.close();
        }
    }
}
