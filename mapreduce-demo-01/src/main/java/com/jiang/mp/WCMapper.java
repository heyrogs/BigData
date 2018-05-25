package com.jiang.mp;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WCMapper
extends Mapper<LongWritable,Text,Text,LongWritable>{

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        System.out.println("MAP: key -> [ "+key+" ] , value -> ["+value+"].");
        //接收数据
        String line = value.toString();
        //切分数据
        String [] words = line.split(" ");
        for(String word : words){
            context.write(new Text(word),new LongWritable(1));
        }
    }
}
