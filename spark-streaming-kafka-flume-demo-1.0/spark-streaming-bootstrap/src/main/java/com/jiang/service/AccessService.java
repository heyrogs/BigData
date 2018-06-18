package com.jiang.service;

import com.jiang.bean.AccessCount;

import java.io.IOException;
import java.util.List;

/**
 * @author ajiang
 * @create by 18-6-18 下午2:45
 */
public interface AccessService {


    List<AccessCount> accessCounts(String today) throws IOException;

    List<String> getTitles()throws IOException;


}
