package com.atguigu.gmall.publisher.mapper;

import java.util.List;
import java.util.Map;

/**
 * @author zhoums
 * @version 1.0
 * @date 2021/8/31 10:37
 */
public interface OrderMapper {

    //1 查询当日交易额总数
    Double selectOrderAmountTotal(String date);

    //2 查询当日交易额分时明细
    List<Map> selectOrderAmountHourMap(String date);
}
