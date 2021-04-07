package com.motifsing.flink.statistics.orderpay;

import com.motifsing.flink.statistics.orderpay.beans.OrderEvent;
import com.motifsing.flink.statistics.orderpay.beans.OrderResult;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.net.URL;
import java.util.List;
import java.util.Map;

/**
 * @ClassName OrderPayTimeout
 * @Description
 * @Author Motifsing
 * @Date 2021/4/1 10:35
 * @Version 1.0
 **/
public class OrderPayTimeout {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        URL resource = OrderPayTimeout.class.getResource("/OrderLog.csv");
        SingleOutputStreamOperator<OrderEvent>orderEventStream = env.readTextFile(resource.getPath())
                .map(line -> {
                    String[] fields = line.split(",");
                    return new OrderEvent(Long.parseLong(fields[0]), fields[1], fields[2], Long.parseLong(fields[3]));
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OrderEvent>() {
                    @Override
                    public long extractAscendingTimestamp(OrderEvent element) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        // 定义一个匹配模式
        Pattern<OrderEvent, OrderEvent> orderEventPattern = Pattern.<OrderEvent>begin("create")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return "create".equals(value.getEventType());
                    }
                })
                .followedBy("pay")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return "pay".equals(value.getEventType());
                    }
                })
                .within(Time.minutes(15));

        // 定义侧输出流标签，用来标记超时事件
        OutputTag<OrderResult> orderTimeoutTag = new OutputTag<OrderResult>("timeoutOrder") {};

        // 将匹配模式应用到流上
        PatternStream<OrderEvent> patternStream = CEP.pattern(orderEventStream.keyBy(OrderEvent::getOrderId), orderEventPattern);

        // 检出符合匹配条件的复杂事件，进行转换处理
        SingleOutputStreamOperator<OrderResult> resultStream = patternStream
                .select(orderTimeoutTag, new OrderTimeoutSelect(), new OrderPaySelect());

        resultStream.print("payed normally");

        resultStream.getSideOutput(orderTimeoutTag).print("timeout");

        env.execute("order timeout detect job");

    }

    /**
     * @author Motifsing
     * 2021/4/7 20:18
     * 实现自定义的超时事件处理函数
     */
    public static class OrderTimeoutSelect implements PatternTimeoutFunction<OrderEvent, OrderResult> {

        @Override
        public OrderResult timeout(Map<String, List<OrderEvent>> pattern, long timeoutTimestamp) throws Exception {
            Long timeoutOrderId = pattern.get("create").iterator().next().getOrderId();
            return new OrderResult(timeoutOrderId, "timeout " + timeoutTimestamp);
        }
    }

    /**
     * @author Motifsing
     * 2021/4/7 20:18
     * 实现自定义的正常事件处理函数
     */
    public static class OrderPaySelect implements PatternSelectFunction<OrderEvent, OrderResult> {

        @Override
        public OrderResult select(Map<String, List<OrderEvent>> pattern) throws Exception {
            Long payedOrderId = pattern.get("pay").iterator().next().getOrderId();
            return new OrderResult(payedOrderId, "payed");
        }
    }
}
