package com.jiang.stream;

import kafka.serializer.StringDecoder;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import java.util.*;

/**
 * @author ajiang
 * @create by 18-6-7 下午9:57
 */
public class MyKafkaDirectApproach {


    public static void main(String[] args) throws Exception{

        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("streamingKafka2");

        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(5));

        //然后创建一个set,里面放入你要读取的Topic,可以并行读取多个topic
        Set<String> topics = new HashSet();
        topics.add("hello_topic");

        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "ajiang:9092");
        kafkaParams.put("zookeeper.connect", "ajiang:2180");
        kafkaParams.put("group.id", "testStreamingConsumerGroupId");
        kafkaParams.put("auto.offset.reset", "smallest");
        kafkaParams.put("enable.auto.commit", "false");

        JavaPairInputDStream<String, String> directKafkaStream =
                KafkaUtils.createDirectStream(
                        sc,
                        String.class,
                        String.class,
                        StringDecoder.class,
                        StringDecoder.class,
                        kafkaParams,
                        topics);

        JavaDStream<String> words = directKafkaStream.flatMap(stringStringTuple2 -> {
            String [] tuples = stringStringTuple2._2.split("\\|");
            System.out.println("tuples = [" + tuples + "]");
            return Arrays.asList(tuples).iterator();
        });
        words.print();
        sc.start();
        sc.awaitTermination();
    }

}
