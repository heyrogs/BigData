package com.jiang.service.impl;

import com.jiang.bean.AccessCount;
import com.jiang.common.util.AL;
import com.jiang.service.AccessService;
import org.apache.http.client.utils.DateUtils;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;

import static com.jiang.common.contant.ApplicationContant.*;
import static com.jiang.common.util.HbaseUtil.*;


/**
 * @author ajiang
 * @create by 18-6-18 下午2:46
 */
@Service("accessService")
public class IAccessService implements AccessService {

    public static Map<String, String> titleMap = new HashMap<>();

    static
    {
        titleMap.put("1", "china");
        titleMap.put("2", "such");
        titleMap.put("3", "history");
        titleMap.put("4", "english");
        titleMap.put("5", "programmer");
    }

    @Override
    public List<AccessCount> accessCounts(String today) throws IOException {
        Map<String, String> accessMap = query(TABLE_NAME, today, COL_FAMILY, QUALIFIER_0);
        List<AccessCount> accessCounts = new ArrayList<>();
        for(Map.Entry<String, String> entry : accessMap.entrySet()){
            String row = entry.getKey().split("\\-")[1];
            Integer value = Integer.parseInt(entry.getValue());
            accessCounts.add(new AccessCount(titleMap.get(row), value));
        }
        return accessCounts;
    }


    @Override
    public List<String> getTitles() throws IOException{
        List<String> titleList = new ArrayList<>();
        for(Map.Entry<String, String> entry : titleMap.entrySet()){
            Integer key = Integer.parseInt(entry.getKey());
            String value = entry.getValue();
            titleList.add(key-1, value);
        }
        return titleList;
    }
}
