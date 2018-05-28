package com.jiang.example.window;

import com.jiang.common.Cont;
import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * @author jiang
 * <p>
 *     Spark Streaming通过Socket接收流数据并每10秒运行一次用来处理接收到数据，
 *     处理完毕后打印程序启动后单词出现的频度。相比WordCountStateUpdate的实例，
 *     Spark Streaming窗口统计是通过reduceByKeyAndWindow()方法实现的，在该方
 *     法中需要指定窗口时间长度和滑动时间间隔。
 * </p>
 * Create by 18-5-23 下午9:09
 */
public class SparkStreamingWindow {

    public static void main(String[] args) throws InterruptedException{

        final SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("SparkStreamingWindow");


       final JavaStreamingContext sc =
                new JavaStreamingContext(conf, Durations.seconds(5));

        //receive SocketServer data
        JavaReceiverInputDStream<String> receiverInputDStream =
                sc.socketTextStream(Cont.HOST,Cont.PORT);


        //data type transform
        JavaDStream<String> words =
                receiverInputDStream.flatMap(line -> {
                    List<String> dataList = new ArrayList<>();
                    //count text line number
                    String [] lineRows = line.split("\n");
                    System.out.println("lineRows = [" + lineRows.length + "]");
                    for (String lineRow : lineRows) {
                        String [] wordStrs = lineRow.split(",");
                        for (String wordStr : wordStrs) {
                            dataList.add(wordStr);
                        }
                    }
                    return dataList.iterator();
                });


        //action
        JavaPairDStream<String,Integer> wordsPair =
                words.mapToPair(word ->new Tuple2<>(word,1));

        // windows操作, 叠加处理


        JavaPairDStream<String,Integer> wordCountPair =
                wordsPair.reduceByKeyAndWindow(
                        (v1,v2)->v1+v2
                        ,Durations.seconds(15)
                        ,Durations.seconds(10));

        //windows操作 , 增量处理
       /* JavaPairDStream<String,Integer> wordCount2 =
                wordsPair.reduceByKeyAndWindow(
                        (v1,v2)->(v1+v2)
                        ,(v1,v2)->(v1-v2)
                        ,Durations.seconds(Integer.parseInt(args[2]))
                        ,Durations.seconds(Integer.parseInt(args[3])));

        wordCount2.print();*/
       //wordCountPair.foreachRDD(stringIntegerJavaPairRDD -> {
           //这里可执行写入redis，hbase，mysql等操作
       //});
        wordCountPair.print();
        sc.start();
        sc.awaitTermination();
        sc.close();
    }
}
