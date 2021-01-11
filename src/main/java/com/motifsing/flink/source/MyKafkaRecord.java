package com.motifsing.flink.source;

/**
 * @author Motifsing
 */
public class MyKafkaRecord {

    private String record;

    public MyKafkaRecord(String record) {
        this.record = record;
    }

    public String getRecord() {
        return record;
    }

    public void setRecord(String record) {
        this.record = record;
    }

    @Override
    public String toString() {
        return "MyKafkaRecord{" +
                "record='" + record + '\'' +
                '}';
    }
}
