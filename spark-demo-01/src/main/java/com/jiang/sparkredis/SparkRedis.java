package com.jiang.sparkredis;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import redis.clients.jedis.Jedis;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @author jiang
 * <p>
 *     从redis中获取数据
 * </p>
 * Create by 2018/5/22 10:00
 */
public class SparkRedis {


    public static void main(String[] args) {

        Jedis jedis = new Jedis("hadoop1",6379,5000);
        jedis.auth("12345");

        String userStr = jedis.get("user:information");
        String [] users = userStr.split("\\:");
        //使用并行化数据集
        UserInfo userInfo = new UserInfo(users[0],Integer.valueOf(users[1])
                ,users[2],users[3],users[4]);

        //Encoder<UserInfo> userInfoEncoder = Encoders.bean(UserInfo.class);
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("spark redis from redis by java");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<UserInfo> userInfoJavaRDD = sc.parallelize(Arrays.asList(userInfo));
        JavaPairRDD<UserInfo,Integer> userInfoIntegerJavaPairRDD =
                userInfoJavaRDD.mapToPair(userInfo1 ->
                    new Tuple2<>(userInfo1,1)
                );

        userInfoIntegerJavaPairRDD.foreach(userInfoIntegerTuple2 -> {
                UserInfo userInfo1 = userInfoIntegerTuple2._1;
                System.out.println(userInfo1.toString() + " -----> " + userInfoIntegerTuple2._2);
        });
        sc.close();
    }
}
