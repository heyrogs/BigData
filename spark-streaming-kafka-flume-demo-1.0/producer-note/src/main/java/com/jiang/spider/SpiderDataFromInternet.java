package com.jiang.spider;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;

/**
 * java爬虫
 *
 * @author jiang
 * Create by 2018/6/12 15:25
 */
public class SpiderDataFromInternet {

    /**
     * 从网友爬取数据
     * @param toUrl 网页URL
     * @return 返回一个页面格式
     */
    public static String strSpiderData(final String toUrl) {
        URL url = null;
        URLConnection urlConnection = null;
        BufferedReader bufferedReader = null;
        StringBuilder sb = new StringBuilder();
        try {
            url = new URL(toUrl);
            urlConnection = url.openConnection();
            bufferedReader = new BufferedReader(new InputStreamReader(urlConnection.getInputStream()));
            String line = "";
            while ((line = bufferedReader.readLine()) != null) {
                sb.append(line).append("\n");
            }
            bufferedReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return sb.toString();
    }
}
