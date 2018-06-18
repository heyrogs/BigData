package com.jiang.common.util;

import java.util.logging.Logger;

/**
 * @author ajiang
 * @create by 18-6-18 上午1:02
 */
public class AL {

    public static Logger getLogger(Class val1){
        Logger logger = Logger.getLogger(val1.getName());
        return logger;
    }


    public static Logger getLogger(){

        return Logger.getLogger("streaming.boot.information");
    }

}
