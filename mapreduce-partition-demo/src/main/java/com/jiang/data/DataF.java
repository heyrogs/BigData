package com.jiang.data;

import com.jiang.flow.bean.FlowBean;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class DataF implements WritableComparable<DataF> {

    private String oui;
    private String oragnation;


    public int compareTo(DataF o) {
        return 0;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeBytes(oui);
        dataOutput.writeBytes(oragnation);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.oui = dataInput.readLine();
        this.oragnation = dataInput.readLine();
    }
}
