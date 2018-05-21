package com.jiang.sparkhive;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.hive.HiveContext;
import org.omg.CORBA.PUBLIC_MEMBER;

import java.io.File;

/**
 * @author jiang
 * <p>
 * Create by 18-5-20 下午9:54
 */
public class SparkHive {

    private static final String PRO_PATH = System.getProperty("user.dir");
    static final String warehouseLocation = new File("warehouse").getAbsolutePath();

    public static void example01(){

        SparkSession spark = SparkSession.builder()
                .appName("Java spark Hive Example")
                .master("spark://jiang:7077")
                .config("spark.sql.warehouse.dir",warehouseLocation)
                .enableHiveSupport()
                .getOrCreate();

        spark.sql("CREATE DATABASE IF NOT EXISTS hive");
        spark.sql("CREATE TABLE IF NOT EXISTS src(key INT,value STRING) USING hive ");
        spark.sql("LOAD DATA INPATH 'hdfs://jiang:9000/hive/warehouse/hive.db/src/kvi.txt' " +
                " INTO TABLE src");
        spark.sql("SELECT * FROM src").show();

    }


    public static void main(String[] args) throws Exception{


        SparkHive.example01();

    }



}
