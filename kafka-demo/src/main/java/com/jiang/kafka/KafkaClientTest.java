package com.jiang.kafka;

/**
 * @author jiang
 * <p>
 * Create by 18-5-28 下午10:40
 */
public class KafkaClientTest {


    public static void main(String[] args) {

        new KafkaProducer(KafkaProp.TOPIC).start();

        new KafkaCustomer(KafkaProp.TOPIC).start();

    }

}
