package com.jiang.flow;


import com.jiang.flow.bean.FlowBean;
import com.jiang.flow.mr.FlowCountMapper;
import com.jiang.flow.mr.FlowCountReduce;
import com.jiang.flow.mr.sort.FlowSortMapper;
import com.jiang.flow.mr.sort.FlowSortReduce;
import com.jiang.flow.partition.ProvincePartition;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FlowCountMain {

    public static void main(String[] args) throws Exception{

        Configuration conf = new Configuration();
        System.setProperty("hadoop.home.dir","D:\\programmer\\server\\hadoop-2.7.5" );
        Job job = Job.getInstance(conf);
        job.setJarByClass(FlowCountMain.class);
        //set mapper and reducer
        job.setReducerClass(FlowSortReduce.class);
        job.setMapperClass(FlowSortMapper.class);

        //set out mapper format
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        //set reduce output format
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        job.setNumReduceTasks(1);
        job.setPartitionerClass(ProvincePartition.class);

        //set input and out path
        FileInputFormat.setInputPaths(job,new Path(System.getProperty("user.dir") + "/src/main/resources/testdata.txt"));
        FileOutputFormat.setOutputPath(job,new Path("d:/wordcount/output"));
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
