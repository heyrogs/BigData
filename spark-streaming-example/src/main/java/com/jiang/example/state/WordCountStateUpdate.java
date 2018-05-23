package com.jiang.example.state;

import com.jiang.common.Cont;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
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
 * Spark Streaming通过Socket接收流数据并每5秒运行一次用来处理接收到数据，
 * 处理完毕后打印程序启动后单词出现的频度，相比较前面wordcountbyspasrkstream
 * 实例在该实例中各时间段之间状态是相关的.
 * </p>
 * Create by 2018/5/23 16:00
 */
public class WordCountStateUpdate {


    static final Function2<List<Integer>, Optional<Integer>, Optional<Integer>> updateFunction
            = new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {

        private static final long serialVersionUID = 1L;

        /**
         * 实际上对于每个单词，每次batch计算的时候，都会调用这个函数
         * @param values 相当于当前batch中，聚合后，当前key可能有多个参数
         *               例如：(hello,1),(hello,1) 那么这里传入的是(1,1)
         * @param state 表示之前key的状态
         * @return
         * @throws Exception
         */
        @Override
        public Optional<Integer> call(List<Integer> values, Optional<Integer> state) throws Exception {

            //定义全局计数器
            int nuwVal = 0;
            if (state.isPresent()) {
                nuwVal = state.get();
            }

            for (Integer value : values) {
                nuwVal += value;
            }
            return Optional.of(nuwVal);
        }
    };

    public static void main(String[] args) throws InterruptedException{

        SparkConf conf = new SparkConf()
                .setAppName("WordCountStateUpdate")
                .setMaster("local[2]");

        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(5));
        //定义checkpoint目录为当前目录
        sc.checkpoint(Cont.HDFS_HOST + "wordcount_checkpoint");
        JavaReceiverInputDStream<String> receiverDStream =
                sc.socketTextStream(Cont.HOST, Cont.PORT, StorageLevel.MEMORY_AND_DISK());
        //transformation
        JavaDStream<String> saleJavaDStream =
                receiverDStream.flatMap(
                        line -> {
                            List<String> strList = new ArrayList<>();
                            String[] lineRows = line.split("&");
                            for (String lineRow : lineRows) {
                                String[] words = lineRow.split(",");
                                for (String word : words) {
                                    strList.add(word);
                                }
                            }
                            return strList.iterator();
                        }
                );

        //action
        JavaPairDStream<String, Integer> wordPariDStream =
                saleJavaDStream.mapToPair(word -> new Tuple2<>(word, 1));

        //统计从运行以来单词的出现次数
        JavaPairDStream<String, Integer> wordCount =
                wordPariDStream.updateStateByKey((values, state) -> {

                    int newValue = 0;

                    if (state.isPresent()) {
                        newValue = state.get();
                    }

                    for (int value : values) {
                        newValue += value;
                    }
                    return Optional.of(newValue);
                });

        wordCount.print();
        sc.start();
        sc.awaitTermination();
        sc.close();
    }
}
