package com.jiang.bean;


import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;

/**
 * @author jiang
 * <p>
 * Create by 2018/6/12 10:20
 */
@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
public class Word implements Serializable{

    public final Long serializerUID = 1L;

    private String name;
    private Integer length;

    @Override
    public String toString() {
        return "name:" + name + ",length:" + length;
    }
}
