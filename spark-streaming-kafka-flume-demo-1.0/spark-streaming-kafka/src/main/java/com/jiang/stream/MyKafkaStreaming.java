package com.jiang.stream;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;

import static com.jiang.Constant.*;

/**
 * @author ajiang
 * @create by 18-6-11 下午9:43
 */
public class MyKafkaStreaming {


    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName(APP_NAME)
                .setMaster("local[2]");

        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(5));

        //然后创建一个set,里面放入你要读取的Topic,可以并行读取多个topic
        Set<String> topics = new HashSet();
        topics.add(TOPIC);

        //kafka params
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", BROKER_LIST);
        kafkaParams.put("zookeeper.connect", ZK_SERVERS + ZK_ROOT);
        kafkaParams.put("group.id", GROUP_ID);
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
                        topics
                );


        JavaDStream<String> stringJavaDStream = directKafkaStream.map(obj ->{
            String value = obj._2;
            System.out.println("value = ["+value+"]");
            return value;
        });


        JavaPairDStream<String, Integer> stringJavaPairDStream =
                stringJavaDStream.flatMapToPair(obj ->{
                    Tuple2<String,Integer> tuple2 = new Tuple2<>(obj, 1);
                    return Arrays.asList(tuple2).iterator();
                });

        JavaPairDStream<String, Integer> wordCount =
                stringJavaPairDStream.reduceByKey((v1, v2) ->{
                    return (v1 + v2);
                });
        wordCount.print();
        sc.start();

        try {
            sc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
