package com.jiang.sparksql;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

import java.io.IOException;

/**
 * @author jiang
 * <p>
 * Create by 18-5-21 下午9:22
 */
public class JavaBeanOpratorSpark {



    static final SparkSession spark = SparkSession.builder()
            .master("local")
            .appName("javaBeanOprToSpark")
            .config("spark.some.config.option", "some-value")
            .getOrCreate();


   static final String redirFile = System.getProperty("user.dir") + "/src/main/resources";



    public  static void readData(){


        JavaRDD<Person> personJavaRDD = spark.read()
                .textFile(redirFile+"/user.txt")
                .javaRDD()
                .map(line ->{
                    String [] part = line.split("\t");
                    Person person = new Person(part[1],Integer.valueOf(part[0]));
                    return person;
                });


        Dataset<Row> peopleDF = spark.createDataFrame(personJavaRDD,Person.class);
        try {
            peopleDF.createTempView("person");
            Dataset<Row> transDF = spark.sql("SELECT name,age FROM person");

            Encoder<String> stringEncoder = Encoders.STRING();
            Dataset<String> teenagerNamesByIndexDF = transDF.map(
                    (MapFunction<Row,String>)row -> "Name:"+row.getString(0),
                    stringEncoder
            );
            teenagerNamesByIndexDF.show();

        } catch (AnalysisException e) {
            e.printStackTrace();
        }


    }


    public static void main(String[] args){

        JavaBeanOpratorSpark.readData();


    }




}
