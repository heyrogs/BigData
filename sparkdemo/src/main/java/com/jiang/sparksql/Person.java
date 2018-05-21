package com.jiang.sparksql;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;

/**
 * @author jiang
 * <p>
 * Create by 18-5-21 下午9:08
 */
@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class Person implements Serializable{


    private String name;
    private int age;


}
