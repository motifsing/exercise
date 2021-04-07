package com.motifsing.flink.statistics.orderpay;

import com.motifsing.flink.statistics.orderpay.beans.OrderEvent;
import com.motifsing.flink.statistics.orderpay.beans.OrderResult;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.net.URL;

/**
 * @ClassName OrderPayTimeoutWithoutCep
 * @Description
 * @Author Motifsing
 * @Date 2021/4/7 20:52
 * @Version 1.0
 **/
public class OrderPayTimeoutWithoutCep {

    public static final OutputTag<OrderResult> orderTimeoutTag = new OutputTag<OrderResult>("order-timeout"){};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        URL resource = OrderPayTimeout.class.getResource("/OrderLog.csv");
        SingleOutputStreamOperator<OrderEvent> orderEventStream = env.readTextFile(resource.getPath())
                .map(line -> {
                    String[] fields = line.split(",");
                    return new OrderEvent(Long.parseLong(fields[0]), fields[1], fields[2], Long.parseLong(fields[3]));
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OrderEvent>() {
                    @Override
                    public long extractAscendingTimestamp(OrderEvent element) {
                        return element.getTimestamp()*1000;
                    }
                });

        SingleOutputStreamOperator<OrderResult> resultStream = orderEventStream.keyBy(OrderEvent::getOrderId)
                .process(new OrderPayMatchDetect());

        resultStream.print("payed normally");
        resultStream.getSideOutput(orderTimeoutTag).print("timeout");

        env.execute("order timeout detect without cep job");
    }

    /**
     * @author yhw
     * 2021/4/7 20:57
     * 实现自定义处理函数
     */

    public static class OrderPayMatchDetect extends KeyedProcessFunction<Long, OrderEvent, OrderResult> {

        // 定义状态，保存之前订单是否已经来过
        ValueState<Boolean> isCreateState;
        ValueState<Boolean> isPayedState;

        // 定义状态，保存时间戳定时器
        ValueState<Long> timerTsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            isCreateState = getRuntimeContext().getState(new ValueStateDescriptor("is-created", Boolean.class, false));
            isPayedState = getRuntimeContext().getState(new ValueStateDescriptor("is-payed", Boolean.class, false));
            timerTsState = getRuntimeContext().getState(new ValueStateDescriptor("timer-ts", Long.class));
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<OrderResult> out) throws Exception {
            // 定时器触发，说明一定有一个事件没有到来
            if (isPayedState.value()){
                // 如果pay来了，说明create事件没有来
                ctx.output(orderTimeoutTag, new OrderResult(ctx.getCurrentKey(), "payed but not found created log"));
            } else {
                // 如果pay没来，说明支付超时
                ctx.output(orderTimeoutTag, new OrderResult(ctx.getCurrentKey(), "timeout"));
            }
            // 清空状态
            isCreateState.clear();
            isPayedState.clear();
            timerTsState.clear();
        }

        @Override
        public void processElement(OrderEvent value, Context ctx, Collector<OrderResult> out) throws Exception {
            // 先获取当前状态
            Boolean isCreated = isCreateState.value();
            Boolean isPayed = isPayedState.value();
            Long timerTs = timerTsState.value();

            // 判断当前事件类型
            if ("create".equals(value.getEventType())) {
                // 1. 如果来的是create，判断是否支付过
                if (isPayed) {
                    // 1.1 如果已经正常支付，输出正常匹配结果（乱序导致create比pay晚， 正常不会超过15分钟）
                    out.collect(new OrderResult(value.getOrderId(), "payed successfully"));
                    // 清空状态和定时器
                    isCreateState.clear();
                    isPayedState.clear();
                    timerTsState.clear();
                    ctx.timerService().deleteEventTimeTimer(timerTs);
                } else {
                    // 1.2 如果没有支付过，注册一个15分钟的定时器，开始等待
                    long ts = (value.getTimestamp() + 15 * 60) * 1000L;
                    ctx.timerService().registerEventTimeTimer(ts);
                    // 更新状态
                    timerTsState.update(ts);
                    isCreateState.update(true);
                }
            } else if ("pay".equals(value.getEventType())) {
                // 2. 如果来的是pay，判断是否有create事件来过（有可能乱序或者create事件丢失）
                if (isCreated) {
                    // 2.1 有create事件，继续判断pay事件是否超过15分钟
                    if (value.getTimestamp() * 1000L < timerTs) {
                        // 2.1.1 在15分钟内，则正常输出
                        out.collect(new OrderResult(value.getOrderId(), "payed successfully"));
                    } else {
                        // 2.1.2 已经超时，输出到侧输出流中
                        ctx.output(orderTimeoutTag, new OrderResult(value.getOrderId(), "payed but already timeout"));
                    }
                    // 清空状态和定时器
                    isCreateState.clear();
                    isPayedState.clear();
                    timerTsState.clear();
                    ctx.timerService().deleteEventTimeTimer(timerTs);
                } else {
                    // 2.2 没有create事件，乱序，注册一个定时器，等待create事件到来
                    ctx.timerService().registerEventTimeTimer(value.getTimestamp() * 1000L);
                    // 更新状态
                    timerTsState.update(value.getTimestamp() * 1000L);
                    isPayedState.update(true);
                }
            }
        }
    }

}
