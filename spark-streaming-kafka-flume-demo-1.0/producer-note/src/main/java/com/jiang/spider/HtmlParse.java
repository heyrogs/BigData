package com.jiang.spider;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;

/**
 * @author jiang
 * <p>
 * Create by 2018/6/13 14:36
 */
public class HtmlParse {

    public static void parse()  throws IOException{
        Document document = Jsoup.connect("http://example.com/").get();
        String title = document.title();
        System.out.println("title = [ " + title +" ] ");
        Elements elements = document.getElementsByTag("a");
        for(Element element: elements){
            String content = element.text();
            String href = element.attr("href");
            System.out.println("content = " + content + " : href : " + href);
        }
    }

    public static void main(String[] args) throws Exception{

       String html =SpiderDataFromInternet.strSpiderData("https://pixabay.com/zh/photos/");
       Document document = Jsoup.parse(html);
       Elements imgs = document.getElementsByTag("img");
       for(Element img : imgs){
           String src = img.attr("src");
           String alt = img.attr("alt");
           System.out.println("url = [" + src + "], alt : " + alt);
       }
    }
}
