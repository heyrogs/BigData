package com.jiang.mp;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WCReduce
        extends Reducer<Text,LongWritable,Text, LongWritable>{

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

        System.out.println("REDUCE: key -> [ "+key+" ] , value -> ["+values+"].");
        //定义一个计算器
        long counter = 0;
        for(LongWritable i : values){
            counter+=i.get();
        }
        //输出
        context.write(key,new LongWritable(counter));
    }
}
