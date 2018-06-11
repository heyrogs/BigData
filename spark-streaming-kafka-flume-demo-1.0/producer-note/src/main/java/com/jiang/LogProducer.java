package com.jiang;

import org.apache.log4j.Logger;

/**
 * @author ajiang
 * @create by 18-6-11 下午8:24
 */
public class LogProducer {


    public static final Logger log = Logger.getLogger(LogProducer.class);

    public static void main(String[] args) {

        int i = 0;

        while (true){

            log.info("value  : " + i);
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
          i++;
        }
    }
}
