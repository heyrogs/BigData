package com.jiang.sparkredis;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;

/**
 * @author jiang
 * <p>
 * Create by 2018/5/22 10:00
 */
@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class UserInfo implements Serializable{

    private String name;
    private Integer age;
    private String city;
    private String phone;
    private String country;


    @Override
    public String toString() {
        return "{\"name\":\""+name+"\",\\\"age\\\":\\\"\"+age+\"\\\"" +
                ",\\\"city\\\":\\\"\"+city+\"\\\",\\\"phone\\\":\\\"\"+phone+\"\\\"," +
                "\\\"country\\\":\\\"\"+country+\"\\\",}";
    }
}
