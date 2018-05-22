package com.jiang.example.sale;

import com.jiang.BaseSparkStream;
import jdk.nashorn.internal.runtime.logging.Logger;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @author jiang
 * <p>
 *     在该实例中将由4.1流数据模拟器以1秒的频度发送模拟数据（销售数据），
 *     Spark Streaming通过Socket接收流数据并每5秒运行一次用来处理接收到数据，
 *     处理完毕后打印该时间段内销售数据总和，需要注意的是各处理段时间之间状态并无关系。
 * </p>
 * data format:订单号、行号、货品、数量、金额
 * Create by 18-5-22 下午9:54
 */
@Logger
public class SaleAmount extends BaseSparkStream {



    public void saleAmount(String []args){

        JavaStreamingContext sc = this.streamingContext();

        //通过Socket获取数据，该处需要提供Socket的主机名和端口号，数据保存在内存和硬盘中
        JavaReceiverInputDStream<String> lines =
                sc.socketTextStream("jiang",9999);

        //spit line to word
        JavaDStream<Sale> words = lines.flatMap(line -> {
            String [] strs = line.split(",");
            if(strs.length == 6){
                //string[] -> object
                Sale sale = new Sale(args[0],args[1],args[2]
                        ,Integer.parseInt(args[3]),Double.parseDouble(args[4]));
                return Arrays.asList(sale).iterator();
            }
            return null;
        });


        JavaPairDStream<String,Double> wordCount = words.mapToPair(sale
                -> new Tuple2<>("count",sale.getMoney())).reduceByKey((v1,v2)->{
                    System.out.println(v1 + " : " + v2);
                    return (v1+v2);
        });


        wordCount.print();
        sc.start();

        try {
            sc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {
        new SaleAmount().saleAmount(args);


    }


}
