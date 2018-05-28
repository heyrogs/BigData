package com.jiang.flow.partition;

import com.jiang.flow.bean.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;
import java.util.Map;

/**
 *  自定义分组策略 -----> 运行在mapper排序完成后的碎片切割过程
 */
public class ProvincePartition
        extends Partitioner<FlowBean,Text>{

       public static Map<String,Integer> provinceMap = new HashMap<String, Integer>();

       static
       {
           provinceMap.put("gz",0);
           provinceMap.put("sh",1);
           provinceMap.put("bj",2);
           provinceMap.put("zj",3);
       }

    @Override
    public int getPartition(FlowBean key, Text value, int i) {
           System.out.println("----->province : " + key.toString());
           Integer code = provinceMap.get(key.getProvince());
           return null==code?3:code;
    }
}
