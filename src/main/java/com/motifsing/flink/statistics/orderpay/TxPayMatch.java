package com.motifsing.flink.statistics.orderpay;

import com.motifsing.flink.statistics.orderpay.beans.OrderEvent;
import com.motifsing.flink.statistics.orderpay.beans.ReceiptEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.net.URL;

/**
 * @ClassName TxPayMatch
 * @Description
 * @Author Motifsing
 * @Date 2021/4/8 9:45
 * @Version 1.0
 **/
public class TxPayMatch {

    private static final OutputTag<ReceiptEvent> NO_MATCH_RECEIPT_EVENT_TAG = new OutputTag<ReceiptEvent>("no-match-receipt"){};
    private static final OutputTag<OrderEvent> NO_MATCH_PAY_EVENT_TAG = new OutputTag<OrderEvent>("no-match-pay"){};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        URL orderResource = TxPayMatch.class.getResource("/OrderLog.csv");
        SingleOutputStreamOperator<OrderEvent> orderEventStream = env.readTextFile(orderResource.getPath())
                .map(line -> {
                    String[] fields = line.split(",");
                    return new OrderEvent(Long.parseLong(fields[0]), fields[1], fields[2], Long.parseLong(fields[3]));
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OrderEvent>() {
                    @Override
                    public long extractAscendingTimestamp(OrderEvent element) {
                        return element.getTimestamp() * 1000L;
                    }
                })
                .filter(data -> !"".equals(data.getTxId()));

        URL receiptResource = TxPayMatch.class.getResource("/ReceiptLog.csv");
        SingleOutputStreamOperator<ReceiptEvent> receiptEventStream = env.readTextFile(receiptResource.getPath())
                .map(line -> {
                    String[] fields = line.split(",");
                    return new ReceiptEvent(fields[0], fields[1], Long.parseLong(fields[2]));
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<ReceiptEvent>() {
                    @Override
                    public long extractAscendingTimestamp(ReceiptEvent element) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        SingleOutputStreamOperator<Tuple2<OrderEvent, ReceiptEvent>> resultStream = orderEventStream
                .keyBy(OrderEvent::getTxId)
                .connect(receiptEventStream.keyBy(ReceiptEvent::getTxId))
                .process(new TxPayMatchDetect());

        resultStream.print("match-pays");

        resultStream.getSideOutput(NO_MATCH_RECEIPT_EVENT_TAG).print("un-match-receipts");
        resultStream.getSideOutput(NO_MATCH_PAY_EVENT_TAG).print("un-match-pays");

        env.execute("tx match detect job");
    }

    /**
     * @author Motifsing
     * 2021/4/8 10:15
     * 实现自定义的process
     */
    public static class TxPayMatchDetect extends CoProcessFunction<OrderEvent, ReceiptEvent, Tuple2<OrderEvent, ReceiptEvent>> {

        // 定义状态，保存支付和到账事件
        ValueState<OrderEvent> payState;
        ValueState<ReceiptEvent> receiptState;

        @Override
        public void open(Configuration parameters) throws Exception {
            payState = getRuntimeContext().getState(new ValueStateDescriptor<>("pay", OrderEvent.class));
            receiptState = getRuntimeContext().getState(new ValueStateDescriptor<>("receipt", ReceiptEvent.class));
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
            // 定时器触发，说明有一个事件没有到来或者是都来了，但是状态被清空了（此时说明事件都到了，不用处理）
            // 只要判断某个事件不为空，就证明另一个事件没有到来
            if (payState.value() != null) {
                ctx.output(NO_MATCH_PAY_EVENT_TAG, payState.value());
            }

            if (receiptState.value() != null) {
                ctx.output(NO_MATCH_RECEIPT_EVENT_TAG, receiptState.value());
            }

            // 清空状态
            payState.clear();
            receiptState.clear();
        }

        @Override
        public void processElement1(OrderEvent pay, Context ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
            // 判断到账事件是否到来
            ReceiptEvent receipt = receiptState.value();
            if (receipt != null) {
                // 如果receipt事件到来，正常输出，清空状态
                out.collect(Tuple2.of(pay, receipt));
                // 清空状态
                receiptState.clear();
                payState.clear();
            } else {
                // 如果receipt没有来，注册一个定时器，等待receipt
                ctx.timerService().registerEventTimeTimer((pay.getTimestamp() + 5) * 1000L);
                // 更新状态
                payState.update(pay);
            }

        }

        @Override
        public void processElement2(ReceiptEvent receipt, Context ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
            // 判断支付事件是否到来
            OrderEvent pay = payState.value();
            if (pay != null) {
                // 如果pay事件到来，正常输出，清空状态
                out.collect(Tuple2.of(pay,receipt));
                // 清空状态
                payState.clear();
                receiptState.clear();
            } else {
                // 如果pay事件没有到来，乱序，注册一个定时器，等待pay
                ctx.timerService().registerEventTimeTimer((receipt.getTimestamp() + 3) * 1000);
                // 更新状态
                receiptState.update(receipt);
            }
        }
    }
}
