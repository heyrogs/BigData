package com.jiang.example.sale;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;

/**
 * @author jiang
 * <p>
 * Create by 18-5-22 下午10:25
 */
@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
public class Sale implements Serializable {

    //订单号、行号、货品、数量、金额
    private String saleNo;
    private String rowNo;
    private String product;
    private int productNum;
    private double money;

    @Override
    public String toString() {
        return "saleNo:" + saleNo
                + ",rowNo:" + rowNo
                + ",product:" + product
                + "productNum:" + productNum
                + "money:" + money;
    }
}
