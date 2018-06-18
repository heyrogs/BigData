package com.jiang.controller;

import com.jiang.bean.AccessCount;
import com.jiang.service.AccessService;
import org.apache.http.client.utils.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.ModelAndView;

import java.io.IOException;
import java.util.Date;
import java.util.List;

/**
 * @author ajiang
 * @create by 18-6-18 上午12:13
 */
@RestController
public class IndexController {

    @Autowired
    private AccessService accessService;

    @GetMapping("/hello")
    public String sayHello(){

        return "hello java";
    }

    @GetMapping("/index")
    public ModelAndView index() {

        // to main page
        return new ModelAndView("index");
    }


    @PostMapping("/list")
    public List<AccessCount> list(@RequestParam(required = false) String day)throws IOException{
        if(day == null || day.equals(""))
            day = DateUtils.formatDate(new Date(), "yyyyMMdd");
        // ---> from hbase data
        return accessService.accessCounts(day);
    }


    @PostMapping("/titles")
    public List<String> getTitles()throws IOException{

        //show all title
        return accessService.getTitles();
    }


}
