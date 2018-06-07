package com.jiang.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

import java.util.Arrays;
import java.util.Collections;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sort_array;
import static org.apache.spark.sql.functions.var_pop;

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

    private static final String SOURCE_DIRECTORY = "/opt/testdata/user.json";

    private static SparkSession spark = SparkSession
            .builder()
            .appName("java spark sql basic opr")
            .master("local")
            //.config("spark.sql.warehouse.dir",System.getProperty("user.dir") + "warehouse")
            .config("spark.some.config.option", "some-value")
            .getOrCreate();



    /**
     *
     *
     *
     */
    public static void sparkSql01() {

        //创建DataFrames(非类型化数据集)
        Dataset<Row> df = spark.read().json(SOURCE_DIRECTORY);
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

        //创建DataFrames(非类型化数据集)
        Dataset<Row> df = spark.read().json(SOURCE_DIRECTORY);
        //注册DataFrame为一个sql的临时视图
        df.createOrReplaceTempView("person");
        //使用sql实现数据查询
        Dataset<Row> sqlDF = spark.sql("select count(*) from person where age>20");
        sqlDF.show();
    }

    /**
     * spark sql数据源加载和保存数据的一般方法
     */
    public static void sparkSqlDataSourceSaveAndLoad() {


        //创建DataFrames(非类型化数据集)
        //load
        Dataset<Row> df = spark.read()
                .format("json")
                .json(SOURCE_DIRECTORY);
        //save
        df.select("type").write().format("file").save("save-china.txt");
       // sparkSession.close();
    }


    /**
     *
     *  don't load file , order to select data
     *
     *  save model:
     *  SaveMode.ErrorIfExists(默认)     抛出异常
     *  SaveMode.Append                      数据会以追加的方式保存
     *  SaveMode.Overwrite                   新数据会覆盖原数据(先删除原数据，再保存新数据)
     *  SaveMode.Ignore                        不保存新数据，相当于SQL语句的CREATE TABLE IF NOT EXISTS
     *
     */
    public static void sparkSql03(){


        Dataset<Row> rows = spark.sql("SELECT * FROM json.`/opt/testdata/user.json`");
        rows.cache().show();

        //start use model than save data to user2.json
        //rows.write().mode(SaveMode.Append).save("/opt/testdata/user2.json");


    }



    public static void sparkSql04(){

        //get data and change to RDD format
        Dataset<Row> userDF = spark.read().json(SOURCE_DIRECTORY);
        //change to parquet format
        userDF.write().save("/opt/testdata/user.parquet");
        //get parquet data
        Dataset<Row> parquetDF = spark.read().parquet("/opt/testdata/user.parquet");
        parquetDF.createOrReplaceTempView("qarquetFile");
        Dataset<Row> userInfo = spark.sql("select id,name from qarquetFile");
        userInfo.show();

    }




    public static void main(String[] args) {

        Person person = new Person("andy",32);

        //Encoders are created for java beans
        Encoder<Person> personEncoder = Encoders.bean(Person.class);
        Dataset<Person> javaBeanDS = spark.createDataset(
                Collections.singletonList(person),
                personEncoder
        );
        javaBeanDS.show();


        //------------------------


        Encoder<Integer> integerEncoder = Encoders.INT();
        Dataset<Integer> primitiveDS = spark.createDataset(
                Arrays.asList(1,2,3,4,5),
                integerEncoder
        ).map((MapFunction<Integer, Integer>)value->value+1,integerEncoder);

        primitiveDS.collect();


        //DadaFrames can be convert to a Dataset by providing a class base on name
        //String path = "examples/src/main/resources/people.json";
        //Dataset<Person> peopleDS = spark.read().json(path).as(personEncoder);
        //peopleDS.show();


    }


}
