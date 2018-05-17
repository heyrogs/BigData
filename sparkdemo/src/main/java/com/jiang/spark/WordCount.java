package com.jiang.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

/**
 * @author jiang
 * <p>
 * peocess : file -> rdd -> map
 * </p>
 * Create by 2018/5/17 09:29
 */
public class WordCount {

    static final Pattern SPACE = Pattern.compile("\t");
    static final String LOCAL_FILE_PATH = "D:\\temp\\word";
    static final String HDFS_FILE_PATH = "hdfs://hadoop1:9000/user/sogou/access_log.20060801.decode.filter";

    public static void main(String[] args) {
       // com.jiang.spark.WordCount.wordCount();
        WordCount.wordCount2();
    }


    public static void wordCount() {

        /**
         * 要实现所有spark的应用，需要先初始化SparkContext。
         * 创建的过程中，程序会向集群申请资源及构建相应的运行环境。
         */
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("first run the spark programmer");
        JavaSparkContext sc = new JavaSparkContext(conf);

        /**
         *
         *  使用textFile将hdfs文件装换成RDD
         *  RDD：弹性分布式数据集，即一个 RDD 代表一个被分区的只读数据集。
         */
        //JavaRDD<String> lines = sc.textFile("hdfs://hadoop1:9000/user/sogou/access_log.20060801.decode.filter");
        JavaRDD<String> lines = sc.textFile(LOCAL_FILE_PATH);

        /**
         * 1、这里使用一个flatMap的含义：是将一个rdd分解成多个rdd
         * 例如这里获取到的RDD里面的数据都是一行String，然后使用flatMap将每一行的单词分解出来。
         *
         * 2、 flatMap 和 map的区别：
         *  对于输入：
         *  flatMap会有一个或多个输出
         *  map只是单一的输出
         */
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(SPACE.split(s)).iterator();
            }
        });


        /**
         *
         *  map键值对，类似于MR的map方法
         *  pairFunction(T,K,V): T->输入类型、K，V->输出键值对
         */
        JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {

                //tuple是Scala中的一个对象，call方法的输入参数为T，即：一个单词
                //新的Tuple2对象的key为这个单词,计数为1
                @Override
                public Tuple2<String, Integer> call(String s) throws Exception {
                    return new Tuple2(s, 1);
                }
            });

        /**
         *  1.调用reduceByKey方法，按照key值进行reduce
         *  这里类似于MR的reduce()
         *
         *  2.要求被操作的数据(ones)是KV对形式，这里会对相同的key进行聚合，然后在两两计算
         *
         */
        JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
            //reduce阶段，key相同的value怎么处理的问题
            @Override
            public Integer call(Integer i, Integer i2) throws Exception {
                return i + i2;
            }
        });

        //备注：spark也有reduce方法，输入数据是RDD类型就可以，不需要键值对，
        // reduce方法会对输入进来的所有数据进行两两运算
        List<Tuple2<String, Integer>> output = counts.collect();
        output.forEach(tuple -> System.out.println(tuple._1 + ":" + tuple._2));
        sc.stop();
    }


    /**
     *   步骤内容:
     *   1.从本地 或 hadoop获取源并转换为RDD。 textFile 或 parallelize
     *      -> 这里只是一个单一的rdd
     *
     *   2.将一个RDD具体化。
     *      -> 这里把一个RDD装换为多个RDD，
     *         例如下面使用flatMap将一行数据分成单个单词。
     *
     *   3.将RDD装换成JavaPirRdd过程。
     *       -> 其实就是数据统计 或 计算的过程
     *          这里类似于MR的map 和 reduce 以下使用的是：
     *          .mapToPair(word -> new Tuple2<>(word,1))
                .reduceByKey((x,y) ->x+y);
     *
     *   4.将RDD装换为java集合的过程。
     *   ->使用collect装换为List<Tuple2<String,Integer>>
     */
    public static void wordCount2(){

        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("run simple count");

        try(JavaSparkContext sc = new JavaSparkContext(conf)){
            //获取源，将源分解成单词
            JavaRDD<String> lines = sc.textFile(LOCAL_FILE_PATH)
                    .flatMap(line -> Arrays.asList(SPACE.split(line)).iterator());

            //类似map，reduce的运算过程
            JavaPairRDD<String,Integer> map =
                    lines
                            .mapToPair(word -> new Tuple2<>(word,1))
                            .reduceByKey((x,y) ->x+y);

            List<Tuple2<String,Integer>> tuple2s = map.collect();
            tuple2s.forEach(tuple -> System.out.println(tuple._1 + " - " + tuple._2));
        }
    }
}
