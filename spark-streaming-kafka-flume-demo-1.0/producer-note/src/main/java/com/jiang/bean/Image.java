package com.jiang.bean;


import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Set;

/**
 * @author jiang
 * <p>
 * Create by 2018/6/13 11:10
 */
@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class Image {


    private Set<String> urls;
    private String alt;

    @Override
    public String toString() {
        return "urls:" + urls
                +",alt:" + alt;
    }
}
