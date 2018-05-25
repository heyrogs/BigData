package com.jiang.flow.mr.sort;

import com.jiang.flow.bean.FlowBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

//将对象传到key中，实现排序
public class FlowSortMapper
        extends Mapper<LongWritable,Text,FlowBean,Text> {

    Text key2 = new Text();
    FlowBean flowBean = new FlowBean();

    /**
     *  TextInputFormat从文件中获取数据，然后这里将数据整理到对应的对象中
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        //获取每一行数据，并切割成字符串数组
        String [] args = line.split("\t");

        String phone = args[1];
        String provice = args[2];
        int len = args.length;
        Long upFlow = Long.parseLong(args[len-2]);
        Long downFlow = Long.parseLong(args[len-1]);
        key2.set(phone);
        flowBean.set(upFlow,downFlow,provice);
        context.write(flowBean,key2);
    }

}
