package com.atguigu.gmall.publisher.service;

import java.util.Map;

/**
 * @author zhoums
 * @version 1.0
 * @date 2021/8/30 10:43
 */
public interface PublisherService {
    //获取日活总数数据
    public Integer getDauTotal(String date);
    //获取日活分时数据
    public Map getDauTotalHours(String date);

    //交易额总数
    Double getOrderAmountTotal(String date);

    //交易额分时数据
    Map<String,Double> getOrderAmountHourMap(String date);
}
