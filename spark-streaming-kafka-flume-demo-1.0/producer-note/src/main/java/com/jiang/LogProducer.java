package com.jiang;

import com.jiang.task.SpiderTask;

/**
 * @author ajiang
 * @create by 18-6-11 下午8:24
 */
public class LogProducer {

    public static void main(String[] args) {
        SpiderTask task = new SpiderTask("http://www.paixin.com/", 5000L);
        new Thread(task).start();
    }
}
