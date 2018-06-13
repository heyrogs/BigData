package com.jiang;

import com.jiang.task.SpiderTask;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author ajiang
 * @create by 18-6-11 下午8:24
 */
public class LogProducer {

    public static void main(String[] args) {
        SpiderTask task = new SpiderTask("https://pixabay.com/zh/photos/", 50000L);
        ExecutorService executorService = Executors.newFixedThreadPool(5);
        executorService.submit(task);
    }
}
