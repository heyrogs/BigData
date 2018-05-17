package com.jiang.sparksql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

/**
 * @author jiang
 * <p>
 *     此处对spark sql的基本使用
 *     Datasets类似于RDD，不过它有自己的一套序列化和反序列化机制
 *     Datasets使用了一个专门的编码器Encoder来序列化对象而不是使用Java的序列化，
 *     这些专门的编码器使用的格式允许Spark执行像过滤filtering、排序sorting和哈希hashing等操作而不需要把对象反序列化成字节
 * </p>
 * Create by 2018/5/17 16:06
 */
public class SparkSQL {


    public static void sparkSql01(){

        SparkSession sparkSession = SparkSession
                .builder()
                .appName("java spark sql basic opr")
                .master("local[*]")
                .config("spark.some.config.option","some-value")
                .getOrCreate();

        //创建DataFrames(非类型化数据集)
        Dataset<Row> df = sparkSession.read().json("/china.json");
        df.show(10);

        //非类型化数据集操作

        //以树形格式打印schema
        df.printSchema();
        //选择name列显示
        df.select("type").show();
        //选择所有数据，但是对age加1
        //df.select(col("name"),col("age").plus(1)).show();
        //选择年龄大于21的people
        //df.filter(col("age").gt(21)).show();

    }


    public static void sparkSql02(){

        SparkSession sparkSession = SparkSession
                .builder()
                .appName("java spark sql basic opr")
                .master("local[*]")
                .config("spark.some.config.option","some-value")
                .getOrCreate();

        //创建DataFrames(非类型化数据集)
        Dataset<Row> df = sparkSession.read().json("/china.json");
        //注册DataFrame为一个sql的临时视图
        df.createOrReplaceTempView("people");
        //使用sql实现数据查询
        Dataset<Row> sqlDF = sparkSession.sql("select * from china");
        sqlDF.show();
    }

    public static void main(String[] args) {

        SparkSQL.sparkSql01();

    }


}
