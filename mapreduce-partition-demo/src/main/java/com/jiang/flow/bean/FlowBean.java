package com.jiang.flow.bean;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Setter
@Getter
@NoArgsConstructor
public class FlowBean implements WritableComparable<FlowBean>{

    private Long upFlow;
    private Long downFlow;
    private Long sumFlow;
    private String province;

    //hadoop 的序列化
    public void write(DataOutput dataOutput) throws IOException {
           dataOutput.writeLong(upFlow);
           dataOutput.writeLong(downFlow);
           dataOutput.writeLong(sumFlow);
           dataOutput.writeBytes(province);
    }

    //hadoop的反序列化
    //顺序必须和序列化的一致
    public void readFields(DataInput dataInput) throws IOException {
            this.upFlow = dataInput.readLong();
            this.downFlow = dataInput.readLong();
            this.sumFlow = dataInput.readLong();
            this.province = dataInput.readLine();
    }

    //hadoop的对象排序，返回1表示顺序排序
    //返回0表示逆序排序
    //根据总的flow来排序
    public int compareTo(FlowBean o) {
        return this.sumFlow > o.sumFlow?-1:1;
    }

    //数据设置
    public void set(Long upFlow,Long downFlow,String province){
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow + downFlow;
        this.province = province;
    }

    public void set(Long upFlow,Long downFlow){
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow + downFlow;
    }

    @Override
    public String toString() {
        return this.province+"\t"+this.upFlow + "\t" + this.downFlow + "\t" + this.sumFlow;
    }
}
