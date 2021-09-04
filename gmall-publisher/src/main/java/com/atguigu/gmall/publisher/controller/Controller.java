package com.atguigu.gmall.publisher.controller;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.publisher.service.PublisherService;
import org.joda.time.LocalDate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * @author zhoums
 * @version 1.0
 * @date 2021/8/30 10:47
 */
@RestController
public class Controller {

    @Autowired
    private PublisherService publisherService;

    /**
     * 封装总数数据
     * @param date
     * @return
     */
    @RequestMapping("realtime-total")
    public String realtimeTotal(@RequestParam("date") String date) {
        //从service层获取日活总数数据
        Integer dauTotal = publisherService.getDauTotal(date);
        //从service层获取交易额数据
        Double orderAmountTotal = publisherService.getOrderAmountTotal(date);

        //创建list集合存放最终数据
        ArrayList<Map> result = new ArrayList<>();

        //创建存放新增日活的map集合
        HashMap<String, Object> dauMap = new HashMap<>();
        dauMap.put("id", "dau");
        dauMap.put("name", "新增日活");
        dauMap.put("value", dauTotal);

        //创建存放新增设备的map集合
        HashMap<String, Object> devMap = new HashMap<>();
        devMap.put("id", "new_mid");
        devMap.put("name", "新增设备");
        devMap.put("value", 233);

        //创建存放交易额总数的map集合
        HashMap<String, Object> gmvMap = new HashMap<>();
        gmvMap.put("id","order_amount");
        gmvMap.put("name","新增交易额");
        gmvMap.put("value",orderAmountTotal);

        result.add(dauMap);
        result.add(devMap);
        result.add(gmvMap);

        return JSONObject.toJSONString(result);
    }
    /**
     * 封装分时数据
     * @param id
     * @param date
     * @return
     */
    @RequestMapping("realtime-hours")
    public String realtimeHours(@RequestParam("id") String id,
                                @RequestParam("date") String date) {
        //获取昨天的日期
        String yesterday = LocalDate.parse(date).plusDays(-1).toString();

        Map todayHourMap = null;
        Map yesterdayHourMap = null;
        if ("dau".equals(id)) {
            //获取今天日活数据
            todayHourMap = publisherService.getDauTotalHours(date);

            //获取昨天数据
            yesterdayHourMap = publisherService.getDauTotalHours(yesterday);

        }else if ("order_amount".equals(id)) {
            //获取今天交易额数据
            todayHourMap = publisherService.getOrderAmountHourMap(date);

            //获取昨天交易额数据
            yesterdayHourMap = publisherService.getOrderAmountHourMap(yesterday);
            
        }


        //创建map集合用于存放结果数据
        HashMap<String, Object> result = new HashMap<>();

        result.put("yesterday", yesterdayHourMap);
        result.put("today", todayHourMap);

        return JSONObject.toJSONString(result);
    }
}
