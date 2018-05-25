package com.jiang.mp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WCTest {

    public static void main(String[] args) throws Exception{
        System.setProperty("hadoop.home.dir","D:\\programmer\\server\\hadoop-2.7.5" );
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        //设置jar包运行的主类
        job.setJarByClass(WCTest.class);
        //设置mapper reduce逻辑类
        job.setMapperClass(WCMapper.class);
        job.setReducerClass(WCReduce.class);

        //指定mapTask输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        //指定reduce的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //指定MapReduce程序的输入和输出路径
        FileInputFormat.setInputPaths(job,new Path(System.getProperty("user.dir") + "/src/main/resources"));
        FileOutputFormat.setOutputPath(job,new Path("D:/wordcount/output"));
        //提交任务
        System.exit(job.waitForCompletion(true)?0:1);
    }

}
