package com.jiang.sparkhive;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

/**
 * @author jiang
 * <p>
 * Create by 18-5-20 下午9:54
 */
public class SparkHive {




    private static final SparkSession spark = SparkSession.builder()
            .appName("spark hive use!")
            .master("local")
            .config("spark.sql.warehouse.dir","hdfs://jiang:9000/hive/warehouse")
            .getOrCreate();

    /**
     *
     *  create database , table and load data
     *
     */
    public static void sparkHive01(){


        //craete table use hive sql
        spark.sql("create database if not exists hive");
        spark.sql("use hive");
        spark.sql("create table if not exists src(key int,value string)");
        spark.sql("LOAD DATA LOCAL INPATH '/opt/testdata/kvi.txt' into TABLE src");

        spark.sql("select * from src").show();

    }



    public static void sparkHive02(){
        spark.sql("select * from hive.src").show();
    }



    public static void main(String[] args) {


        SparkHive.sparkHive02();


    }





}
