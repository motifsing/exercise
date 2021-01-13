package com.motifsing.flink.source;

/**
 * @author Motifsing
 */
public class MyKafkaRecord {

    private String record;

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((record == null) ? 0 : record.hashCode());
        return result;
    }

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
