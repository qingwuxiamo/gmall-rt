package com.atguigu.gmall.publisher.mapper;

import java.util.List;
import java.util.Map;

/**
 * @author zhoums
 * @version 1.0
 * @date 2021/8/30 10:37
 */
public interface DauMapper {

    Integer selectDauTotal(String date);

    List<Map> selectDauTotalHourMap(String date);
}
