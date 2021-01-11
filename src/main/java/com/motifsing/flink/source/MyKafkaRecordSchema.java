package com.motifsing.flink.source;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

/**
 * @author Motifsing
 */
public class MyKafkaRecordSchema implements DeserializationSchema<MyKafkaRecord> {

    @Override
    public MyKafkaRecord deserialize(byte[] message) throws IOException {
        return new MyKafkaRecord(new String(message));
    }

    @Override
    public boolean isEndOfStream(MyKafkaRecord nextElement) {
        return false;
    }

    @Override
    public TypeInformation<MyKafkaRecord> getProducedType() {
        return TypeInformation.of(MyKafkaRecord.class);
    }
}
