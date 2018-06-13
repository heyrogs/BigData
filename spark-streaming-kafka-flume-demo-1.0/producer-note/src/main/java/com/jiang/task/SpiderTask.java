package com.jiang.task;

import com.jiang.bean.Image;
import com.jiang.spider.SpiderDataFromInternet;
import org.apache.log4j.Logger;

import java.util.HashSet;
import java.util.Set;
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
    public void run(){

           /*

            \\b -> 空格 或 标点符号（， 、 。）
            \\s -> space tab  enter 键

         */
        try {
            String imgTab = "<img.*? src=\"?(.*?\\.(jpg|gif|bmp|bnp|png))\".*?>";
            Pattern imgTabPattern = Pattern.compile(imgTab);
            String img = "http[s]?://(w+|[a-zA-Z]*).\\w*.\\w*(/\\w*-?\\d*_*\\w*)*.(jpg|png|jpeg)";
            Pattern imgPattern = Pattern.compile(img);
            String alt = "\"\\W*\"";
            Pattern altPattern = Pattern.compile(alt);

            while (true){
                String str = SpiderDataFromInternet.strSpiderData(url);
                String [] datas = str.split("\n");
                log.info("start execute.....");
                for(String data : datas){
                    Matcher matcher = imgTabPattern.matcher(data);
                    while (matcher.find()){
                        String imgTabContent = matcher.group();
                        Matcher imgUrlMatcher = imgPattern.matcher(imgTabContent);
                        Set<String> urls = new HashSet<>();
                        while (imgUrlMatcher.find()){
                            urls.add(imgUrlMatcher.group());
                        }
                        Matcher altMatcher = altPattern.matcher(imgTabContent);
                        String altStr = "";
                        if(altMatcher.find()){
                            altStr = altMatcher.group();
                        }
                        Image image = new Image(urls, altStr);
                        log.info("image infomation:");
                        log.info(image);
                    }
                }
                Thread.sleep(time);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
