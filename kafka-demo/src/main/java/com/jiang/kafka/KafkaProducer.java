package com.jiang.kafka;

import kafka.producer.KeyedMessage;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
import scala.collection.Seq;
import scala.collection.mutable.ArraySeq;

import java.util.Properties;

/**
 * @author jiang
 * <p>
 * Create by 18-5-28 下午10:15
 */
public class KafkaProducer extends Thread{



    private String topic;


    private Producer<Integer,String> producer;


    public KafkaProducer(String topic){
        this.topic = topic;
        Properties properties = new Properties();
        properties.put("metadata.broker.list",KafkaProp.BROKER_LIST);
        properties.put("serializer.class","kafka.serializer.StringEncoder");
        properties.put("request.required.acks","1");
        producer = new Producer(new ProducerConfig(properties));
    }


    @Override
    public void run() {

        int messageNo =  1;

        while (true){
            String message = "message_" + messageNo;
            producer.send(new KeyedMessage(topic,message));
            System.out.println("message = [" + message + "]");
            messageNo++;
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }
}
