package com.jiang.flow.mr;

import com.jiang.flow.bean.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowCountReduce
        extends Reducer<Text,FlowBean,Text,FlowBean>{

    private  FlowBean flow = new FlowBean();

    /**
     *  接收来自map处理后的数据
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context)
            throws IOException, InterruptedException {

        long upFlowCount = 0;
        long downFlowCount = 0;

        for(FlowBean flowBean : values){
            upFlowCount+=flowBean.getUpFlow();
            downFlowCount+=flowBean.getDownFlow();
        }
        flow.set(upFlowCount,downFlowCount);
        context.write(key,flow);
    }
}
