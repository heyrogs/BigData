package com.jiang.kafka;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @author jiang
 * <p>
 * Create by 18-5-28 下午10:42
 */
public class KafkaCustomer extends Thread {


    private String topic;

    public KafkaCustomer(String topic){

        this.topic = topic;

    }


    private ConsumerConnector connector(){
        Properties properties = new Properties();
        properties.put("zookeeper.connect",KafkaProp.ZK);
        properties.put("group.id",KafkaProp.GROUP_ID);
        return Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));
    }


    @Override
    public void run() {

        ConsumerConnector connector = connector();

        Map<String,Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(topic,1);
        Map<String,List<KafkaStream<byte[],byte[]>>> messageStreams = connector.createMessageStreams(topicCountMap);

        KafkaStream<byte[],byte[]> stream = messageStreams.get(topic).get(0); //get receiver data

        ConsumerIterator<byte[],byte[]> iterator = stream.iterator();

        while (iterator.hasNext()){

            String message = new String(iterator.next().message());

            System.out.println("rec : " + message);


        }


    }
}
