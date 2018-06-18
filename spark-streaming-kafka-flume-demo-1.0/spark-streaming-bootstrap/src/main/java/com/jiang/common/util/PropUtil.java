package com.jiang.common.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author ajiang
 * @create by 18-6-18 上午12:53
 */
public class PropUtil {

    private static String root = "/applicationContext.yaml"; //default cofiguration file
    private static Map<String, Object> map = new HashMap<>();


    public static Map<String, Object> loadMap(final String var1){

        if(var1 == null || "".equals(var1)){
            AL.getLogger().info("param is null !");
           return null;
        }

        InputStream iStream = PropUtil.class.getResourceAsStream(var1);
        Properties properties = new Properties();
        try {
            properties.load(iStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return (Map)properties;
    }

    /**
     * use default configuration
     *
     * @return
     */
    public static Map<String, Object> loadMap(){

        return loadMap(root);
    }

}
