package com.jiang.example.sale;

import jdk.nashorn.internal.runtime.logging.Logger;
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
 * 在该实例中将由4.1流数据模拟器以1秒的频度发送模拟数据（销售数据），
 * Spark Streaming通过Socket接收流数据并每5秒运行一次用来处理接收到数据，
 * 处理完毕后打印该时间段内销售数据总和，需要注意的是各处理段时间之间状态并无关系。
 * </p>
 * data format:订单号、行号、货品、数量、金额
 * Create by 18-5-22 下午9:54
 */
@Logger
public class SaleAmount {

    static final SparkConf sparkConf = new SparkConf()
            .setMaster("local[2]")
            .setAppName("streamDataCount");

    public static void saleAmount() {

        JavaStreamingContext sc = new JavaStreamingContext(sparkConf, Durations.seconds(5));
        JavaReceiverInputDStream<String> lines =
                sc.socketTextStream("hadoop1", 9999, StorageLevel.MEMORY_AND_DISK());

        //flagMap 返回一个新的DStream对象
        JavaDStream<Sale> words = lines.flatMap(line -> {
            System.out.println("line ->" + line);
            String lineRows[] = line.split("&");
            List<Sale> saleList = new ArrayList<>();
            for (String lineRow : lineRows) {
                String sales[] = lineRow.split(",");
                Sale sale = new Sale(
                        sales[0],
                        sales[1],
                        sales[2],
                        Integer.parseInt(sales[3]),
                        Double.parseDouble(sales[4]));
                saleList.add(sale);
            }
            return saleList.iterator();
        });

        words.cache();
        words.foreachRDD(saleJavaRDD -> {
            List<Sale> sales = saleJavaRDD.collect();
            sales.forEach(sale -> {
                System.out.println(sale.getRowNo() + "--->" + sale.getMoney());
            });
        });

        JavaPairDStream<String, Double> wordCount =
                words.mapToPair(sale -> new Tuple2<>("count", sale.getMoney()))
                        .reduceByKey((v1, v2) -> {
                            System.out.println(v1 + " : " + v2);
                            return (v1 + v2);
                        });

        wordCount.print();
        sc.start();
        try {
            sc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //sc.close();
    }


    public static void main(String[] args) {
        SaleAmount.saleAmount();
    }


}
