package com.motifsing.kafka;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName SimpleProducer
 * @Description
 * @Author yhw
 * @Date 2020/4/21 10:00
 * @Version 1.0
 */

public class SimpleProducer {
    public static void main(String[] args) throws Exception{
        //checkout arguments length value
        if(args.length == 0){
            System.out.println("Enter topic name");
            return;
        }

        //Assign topicName to string variable
        String topicName = args[0].toString();

        //Create instance for properties to access producer configs
        Properties props = new Properties();

        //Assign proxy id
        // online
//        props.put("bootstrap.servers", "172.20.2.30:19092,172.20.2.32:19092,172.20.2.31:19092");
        // test
        props.put("bootstrap.servers", "172.20.1.1:9092,172.20.1.2:9092,172.20.1.3:9092");

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

        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        int i = 0;
        // The type of key/value needs to match the type of serializer and producer
        while (i >= 0) {

//            String positionId = getMD5(getRandomString(6));
            String positionId = getMD5("aaaa");

//            String posi = new UUID((int)Math.random()*10 + 1, (int)Math.random()*10 + 1).toString();
            int adxId = (int)(Math.random()*3) + 1;
            int multiple = (int)(Math.random()*3) + 1;
            int assetId;
            switch (adxId){
                case 1:
                    assetId = adxId*multiple;
                    break;
                case 2:
                    assetId = adxId*multiple + 2;
                    break;
                default:
                    assetId = adxId*multiple + 5;
            }

            String[] mtList = {"12001", "12002", "12003", "12004"};
            int mtIndex = (int)(Math.random()*4);
            String  mt = mtList[mtIndex];

            String msg = positionId+ "\t" + adxId + "\t" + assetId + "\t" + mt;
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicName, Integer.toString(i), msg);
            // Asynchronous send
            producer.send(record);
            // Synchronous send
//            producer.send(record).get();
//            System.out.println("Sent message: (" + i + ", " + msg + ")");
//            TimeUnit.SECONDS.sleep(1);
            TimeUnit.MILLISECONDS.sleep(1);
            i++;
        }
        producer.close();
        System.out.println("Message sent successfully!");
    }


    public static String getRandomString(int length) {
        //随机字符串的随机字符库
        String KeyString = "abcdeg";
        StringBuilder str = new StringBuilder();
        int len = KeyString.length();
        for (int i = 0; i < length; i++) {
            str.append(KeyString.charAt((int) Math.round(Math.random() * (len - 1))));
        }
        return str.toString();
    }

    public static String getMD5(String str) {
        try {
            // 生成一个MD5加密计算摘要
            MessageDigest md = MessageDigest.getInstance("MD5");
            // 计算md5函数
            md.update(str.getBytes());
            // digest()最后确定返回md5 hash值，返回值为8为字符串。因为md5 hash值是16位的hex值，实际上就是8位的字符
            // BigInteger函数则将8位的字符串转换成16位hex值，用字符串来表示；得到字符串形式的hash值
            String md5=new BigInteger(1, md.digest()).toString(16);
            //BigInteger会把0省略掉，需补全至32位
            return fillMD5(md5);
        } catch (Exception e) {
            throw new RuntimeException("MD5加密错误:"+e.getMessage(),e);
        }
    }

    public static String fillMD5(String md5){
        return md5.length()==32?md5:fillMD5("0"+md5);
    }
}
