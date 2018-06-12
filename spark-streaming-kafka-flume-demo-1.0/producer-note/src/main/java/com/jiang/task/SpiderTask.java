package com.jiang.task;

import com.jiang.spider.SpiderDataFromInternet;
import org.apache.log4j.Logger;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author jiang
 * <p>
 * Create by 2018/6/12 16:28
 */
public class SpiderTask implements Runnable{

    public static final Logger log = Logger.getLogger(SpiderTask.class);

    private String url;
    private Long time;

    public SpiderTask(String url, Long time){
        this.url = url;
        this.time = time;
    }

    @Override
    public void run() {

        String regex = "http://[\\w+\\.?/?]+\\.[A-Za-z]+";
        Pattern pattern = Pattern.compile(regex);

        while (true){
            String str = SpiderDataFromInternet.strSpiderData(url);
            String [] datas = str.split("\n");
            log.info("start execute.....");
            for(String data : datas){
                Matcher matcher = pattern.matcher(data);
                if(matcher.find()){
                    String group = matcher.group();
                    log.info("info : " + group);
                }
            }

            try {
                Thread.sleep(time);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    }
}
