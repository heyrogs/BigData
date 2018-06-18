package com.jiang.bean;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

/**
 * @author ajiang
 * @create by 18-6-18 下午2:43
 */
@Setter
@Getter
@AllArgsConstructor
public class AccessCount implements Serializable {

    private String name;
    private Integer value;

    @Override
    public String toString() {
        return "{name:'"+name+"',value:"+value+"}";
    }
}
