package com.jiang.flow.mr.sort;

import com.jiang.flow.bean.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowSortReduce
        extends Reducer<FlowBean,Text,Text,FlowBean>{

   // private  FlowBean flow = new FlowBean();

    /**
     *  接收来自map处理后的数据
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        context.write(values.iterator().next(),key);
    }
}
