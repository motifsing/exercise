package com.motifsing.flink.statistics.orderpay;

import com.motifsing.flink.statistics.orderpay.beans.OrderEvent;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.net.URL;

/**
 * @ClassName OrderPayTimeout
 * @Description
 * @Author Motifsing
 * @Date 2021/4/1 10:35
 * @Version 1.0
 **/
public class OrderPayTimeout {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        URL resource = OrderPayTimeout.class.getResource("/OrderLog.csv");
        env.readTextFile(resource.getPath())
                .map(line -> {
                    String[] fields = line.split(",");
                    return new OrderEvent(Long.parseLong(fields[0]), fields[1], fields[2], Long.parseLong(fields[3]));
                });
//                .assignTimestampsAndWatermarks( )

    }
}
