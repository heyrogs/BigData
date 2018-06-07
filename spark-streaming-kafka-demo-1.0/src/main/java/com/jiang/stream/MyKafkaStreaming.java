package com.jiang.stream;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;

/**
 * @author ajiang
 * @create by 18-6-7 下午8:33
 */
public class MyKafkaStreaming {


    public static void main(String[] args) throws Exception{

        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("streamingKafka");

        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(5));

        String consumerGroupId = "testStreamingConsumerGroupId";
        String zkQuorum = "ajiang:2180";
        Map<String,Integer> map = new HashMap<>();
        map.put("hello_topic", 1);
        JavaPairReceiverInputDStream<String, String> receiverInputDStream =
                KafkaUtils.createStream(sc, zkQuorum, consumerGroupId, map);

        JavaDStream<String> javaDStream = receiverInputDStream.map(stream -> {
            Tuple2<String, String> tuple2 = stream;
            String msg = tuple2._2;
            System.out.println("msg = [" + msg + "]");
            return msg;
        });
        javaDStream.print();
        sc.start();
        sc.awaitTermination();
    }



}
