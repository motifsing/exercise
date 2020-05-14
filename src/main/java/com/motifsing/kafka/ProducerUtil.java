package com.motifsing.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName Producer
 * @Description JAVA版本kafka生产者
 * @Author Administrator
 * @Date 2020/5/14 9:28
 * @Version 1.0
 **/
public class ProducerUtil {

    public static void main(String[] args) throws InterruptedException {
        String bootstrapServers = "172.20.1.1:9092,172.20.1.2:9092,172.20.1.3:9092";
        String topicName = "yhw_test";
        Producer<String, String> producer = getKafkaProducer(bootstrapServers);
        int i = 0;
        String[] array = {"spark", "flink", "zookeeper", "hive", "hdfs", "kafka", "flume", "hadoop", "phoenix", "hbase"};
        System.out.println("start producer...");
        while (i < 1000000){
            int random = (int)(1 + Math.random()*(9));
            String key = Integer.toString(i);
            String element = array[random];
            Date dNow = new Date( );
            SimpleDateFormat ft = new SimpleDateFormat ("yyyy-MM-dd hh:mm:ss");
            String value = element + '\t' + ft.format(dNow);
            i++;
            ProducerRecord<String, String> record = getRecords(topicName, key, value);
            System.out.println("record: " + record);
            sendRecord(producer, record);
            TimeUnit.MILLISECONDS.sleep(1000);
        }
        closeProducer(producer);
        System.out.println("close producer...");
    }

    public static Producer<String, String> getKafkaProducer(String bootstrapServers) {
        Properties props = new Properties();

        props.put("bootstrap.servers", bootstrapServers);

        //Set acknowledgements for producer requests
        props.put("acks", "all");

        //If the request fails, the producer can automatically retry
        props.put("retries", 0);

        //Specify buffer size in config
        props.put("batch.size", 16384);

        //Reduce the no of requests less than 0
        props.put("linger.ms", 1);

        //The buffer.memory controls the total amount of memory available to the producer for buffering.
        props.put("buffer.memory", 33554432);

        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        return new KafkaProducer<String, String>(props);
    }

    public static ProducerRecord<String, String> getRecords(String topicName, String key, String value) {

        return new ProducerRecord<String, String>(topicName, key, value);
    }

    public static void sendRecord(Producer<String, String> producer, ProducerRecord<String, String> record){
        producer.send(record);
    }

    public static void closeProducer(Producer<String, String> producer){
        producer.close();
    }

}
