package com.jiang.sparkhive;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.hive.HiveContext;

/**
 * @author jiang
 * <p>
 * Create by 18-5-20 下午9:54
 */
public class SparkHive {

    private static final String PRO_PATH = System.getProperty("user.dir");
    static final SparkSession spark = SparkSession.builder()
            .appName("spark hive use!")
            .master("local[2]")
            .config("spark.sql.warehouse.dir","hdfs://hadoop1:9000/user/hive/warehouse")
            .enableHiveSupport()
            .getOrCreate();
    /**
     *
     *  create database , table and load data
     *
     */
    public static void sparkHiveCreateBaseAndTable(){
        //craete table use hive sql
        spark.sql("create database if not exists hive ");
        spark.sql("use hive ");
        spark.sql("create table if not exists user(id int,name string) ");
        spark.sql("LOAD DATA LOCAL INPATH '"+PRO_PATH+
                "/src/main/resources/user.txt' INTO TABLE user ");
        spark.sql("select * from hive.user ").show();

    }


    public static void queryData(){
        spark.sql("SELECT * FROM user_access_sogo").show();
    }



    public static void main(String[] args) {

        SparkHive.queryData();

    }





}
