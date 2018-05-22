package com.jiang.wordcount;

import com.jiang.BaseSparkStream;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @author jiang
 * <p>
 *     word count
 * </p>
 * Create by 18-5-22 下午8:45
 */
public class WordCountBySparkStream extends BaseSparkStream {



     public void wordCount(final String [] args) throws InterruptedException{

         JavaStreamingContext sc = this.streamingContext();

         JavaReceiverInputDStream<String> lines
                 = sc.socketTextStream("192.168.0.105",9999);

         JavaDStream<String> words = lines.flatMap(line
                 -> Arrays.asList(line.split("\t")).iterator());

         JavaPairDStream<String,Integer> wordCount =
                 words.mapToPair(word -> new Tuple2<>(word,1))
                 .reduceByKey((v1,v2)->(v1 + v2));

         wordCount.print();
         sc.start();
         sc.awaitTermination();
         if(sc != null)sc.close();
     }


    /**
     *
     *  run app
     *
     * @param args
     * @throws InterruptedException
     */
    public static void main(String[] args) throws InterruptedException{

         new WordCountBySparkStream().wordCount(args);

    }



}
