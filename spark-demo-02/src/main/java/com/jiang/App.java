package com.jiang;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author jiang
 * @date 2018/6/25 09:35
 */
public class App {

    static final SparkConf conf = new SparkConf();
    static
    {
         conf.setMaster("local");
         conf.setAppName("mySparkAppliction");
         conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
         conf.set("spark.kryo.registrator", "com.jiang.MyRegistrator");
    }



    public static void main(String[] args) {

        final JavaSparkContext jsc =  new JavaSparkContext(conf);
        execute(jsc);
    }

    //从集合转换为RDD
    public static void execute(final JavaSparkContext jsc) {
        List<String> dataList = new ArrayList<>();
        dataList.add("a,2,e,4,5");
        dataList.add("a,b,c,d,e");
        JavaRDD<String> list = jsc.parallelize(dataList);
        //transform
        /*JavaRDD<String [] > mapRdd = list.map(str -> str.split("\\,"));
        List<String[]> dataList2 = mapRdd.collect();
*/
        JavaRDD<String> flatMapRdd = list.flatMap(s -> Arrays.asList(s.split("\\,")).iterator());

        JavaPairRDD<String, Object> javaPairRDD =
                flatMapRdd.mapToPair((PairFunction<String, String, Object>) str -> new Tuple2<>(str, 1));

       /*

         //groupByKey
        JavaPairRDD<String, Iterable<Integer>> stringIterableJavaPairRDD = javaPairRDD.groupByKey();
        List<Tuple2<String, Iterable<Integer>>>  tuple2s = stringIterableJavaPairRDD.collect();
        tuple2s.forEach(tuple -> System.err.println("key : " + tuple._1 + ", value :" + tuple._2) );
*/

       /*

       //reduceByKey
       JavaPairRDD<String,Object> reduceByKeyRdd =  javaPairRDD.reduceByKey((v1, v2) -> (v1+"|"+v2));
        List<Tuple2<String, Object>> tuple2s = reduceByKeyRdd.collect();
        tuple2s.forEach(tuple -> System.err.println("key : " + tuple._1 + ", value :" + tuple._2) );

       */

       JavaPairRDD<String, String> mapValueRdd = javaPairRDD.mapValues(v -> v + "0");
       List<Tuple2<String, String>> tuple2List = mapValueRdd.collect();
       tuple2List.forEach(tuple -> System.out.println("key : " + tuple._1 + ", value :" + tuple._2));
    }
}
