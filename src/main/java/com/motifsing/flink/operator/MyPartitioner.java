package com.motifsing.flink.operator;

import com.motifsing.flink.source.MyKafkaRecord;
import org.apache.flink.api.common.functions.Partitioner;

public class MyPartitioner implements Partitioner<MyKafkaRecord> {

    @Override
    public int partition(MyKafkaRecord key, int numPartitions) {
        String record = key.getRecord();

        String[] split = record.split("_");

        int len = 2;

        if (split.length == len){
            return new Integer(split[0]);
        } else {
            return 0;
        }
    }
}
