package com.jiang.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

/**
 * @author jiang
 * <p>
 * 此处对spark sql的基本使用
 * Datasets类似于RDD，不过它有自己的一套序列化和反序列化机制
 * Datasets使用了一个专门的编码器Encoder来序列化对象而不是使用Java的序列化，
 * 这些专门的编码器使用的格式允许Spark执行像过滤filtering、排序sorting和哈希hashing等操作而不需要把对象反序列化成字节
 * </p>
 * Create by 2018/5/17 16:06
 */
public class SparkSQL {

    private static final String SOURCE_DIRECTORY = "E://data/person.json";
    private  static String warehouseLocation = System.getProperty("user.dir") + "spark-warehouse";

    /**
     *
     *
     *
     */
    public static void sparkSql01() {

        SparkSession sparkSession = SparkSession
                .builder()
                .appName("java spark sql basic opr")
                .master("local[*]")
                .config("spark.some.config.option", "some-value")
                .config("spark.sql.warehouse.dir",warehouseLocation)
                .getOrCreate();

        //创建DataFrames(非类型化数据集)
        Dataset<Row> df = sparkSession.read().json(SOURCE_DIRECTORY);
        df.show();


        //非类型化数据集操作

        //以树形格式打印schema
        System.out.println("以树形格式打印schema");
        df.printSchema();

        //选择name，age列显示
        df.select("name","age").show();


        //选择所有数据，但是对age加100
        df.select(col("name"),col("age").plus(100)).show();

        //选择年龄大于21的people
        //df.filter(col("age").gt(21)).show();

    }


    public static void sparkSql02() {
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("java spark sql basic opr")
                .master("local[*]")
                .config("spark.some.config.option", "some-value")
                .config("spark.sql.warehouse.dir",warehouseLocation)
                .getOrCreate();

        //创建DataFrames(非类型化数据集)
        Dataset<Row> df = sparkSession.read().json(SOURCE_DIRECTORY);
        //注册DataFrame为一个sql的临时视图
        df.createOrReplaceTempView("person");
        //使用sql实现数据查询
        Dataset<Row> sqlDF = sparkSession.sql("select count(*) from person where age>20");
        sqlDF.show();
    }

    /**
     * spark sql数据源加载和保存数据的一般方法
     */
    public static void sparkSqlDataSourceSaveAndLoad() {

        SparkConf conf = new SparkConf()
                .setMaster("local")
                .set("spark.sql.warehouse.dir",warehouseLocation)
                .setAppName("java spark sql basic opr");
        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();

        //创建DataFrames(非类型化数据集)
        //load
        Dataset<Row> df = sparkSession.read()
                .format("json")
                .json(SOURCE_DIRECTORY);
        //save
        df.select("type").write().format("file").save("save-china.txt");
       // sparkSession.close();
    }


    public static void main(String[] args) {
        SparkSQL.sparkSql02();
    }


}
