package com.atguigu.gmall.publisher.service.impl;

import com.atguigu.constants.GmallConstants;
import com.atguigu.gmall.publisher.bean.Option;
import com.atguigu.gmall.publisher.bean.Stat;
import com.atguigu.gmall.publisher.mapper.DauMapper;
import com.atguigu.gmall.publisher.mapper.OrderMapper;
import com.atguigu.gmall.publisher.service.PublisherService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author zhoums
 * @version 1.0
 * @date 2021/8/30 10:44
 */
@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    private DauMapper dauMapper;
    
    @Autowired
    private OrderMapper orderMapper;

    @Autowired
    private JestClient jestClient;

    @Override
    public Integer getDauTotal(String date) {
        return dauMapper.selectDauTotal(date);
    }

    @Override
    public Map getDauTotalHours(String date) {
        //从mapper层获取数据
        List<Map> list = dauMapper.selectDauTotalHourMap(date);

        //创建map集合存放结果数据
        HashMap<String, Long> result = new HashMap<>();

        for (Map map : list) {
            result.put((String) map.get("LH"), (Long) map.get("CT"));
        }

        return result;
    }

    @Override
    public Double getOrderAmountTotal(String date) {
        return orderMapper.selectOrderAmountTotal(date);
    }

    @Override
    public Map<String, Double> getOrderAmountHourMap(String date) {
        List<Map> list = orderMapper.selectOrderAmountHourMap(date);

        //创建新的map用于结果数据
        Map<String, Double> result = new HashMap<>();

        for (Map map : list) {
            result.put((String) map.get("CREATE_HOUR"),(Double) map.get("SUM_AMOUNT"));
        }
        return result;
    }

    @Override
    public Map<String, Object> getSaleDetail(String date, int startpage, int size, String keyword) throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //过滤 匹配
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("dt", date));
        boolQueryBuilder.must(new MatchQueryBuilder("sku_name", keyword).operator(MatchQueryBuilder.Operator.AND));
        searchSourceBuilder.query(boolQueryBuilder);

        //  性别聚合
        TermsBuilder genderAggs = AggregationBuilders.terms("groupby_user_gender").field("user_gender").size(2);
        searchSourceBuilder.aggregation(genderAggs);

        //  年龄聚合
        TermsBuilder ageAggs = AggregationBuilders.terms("groupby_user_age").field("user_age").size(100);
        searchSourceBuilder.aggregation(ageAggs);

        // 行号= （页面-1） * 每页行数
        searchSourceBuilder.from((startpage - 1) * size);
        searchSourceBuilder.size(size);

        System.out.println(searchSourceBuilder.toString());

        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstants.ES_QUERY_INDEXNAME).addType("_doc").build();

        SearchResult searchResult = jestClient.execute(search);

        //1.获取命中数据条数
        Long total = searchResult.getTotal();

        //2.获取数据明细
        //创建list集合用于存放明细数据
        List<Map> detail = new ArrayList<>();
        List<SearchResult.Hit<Map, Void>> hits = searchResult.getHits(Map.class);
        for (SearchResult.Hit<Map, Void> hit : hits) {
            detail.add(hit.source);
        }

        //3.获取聚合组数据
        MetricAggregation aggregations = searchResult.getAggregations();

        //年龄
        TermsAggregation groupby_user_age = aggregations.getTermsAggregation("groupby_user_age");
        List<TermsAggregation.Entry> buckets = groupby_user_age.getBuckets();
        //20岁以下     30岁及其以上
        int low20Count = 0;
        int high30Count = 0;
        for (TermsAggregation.Entry bucket : buckets) {
            if (Integer.parseInt(bucket.getKey()) <20) {
                low20Count += bucket.getCount();
            }else if (Integer.parseInt(bucket.getKey()) >= 30){
                high30Count += bucket.getCount();
            }
        }
        //计算占比 20岁以下的占比  30岁及其以上的占比
        double low20Ratio = Math.round(low20Count * 1000 / total) / 10D;
        double high30Ratio = Math.round(high30Count * 1000 / total) / 10D;
//        double high20Low30Ratio = 100 - low20Ratio - high30Ratio;
        //这样精度更高
        double high20Low30Ratio = Math.round((100D - low20Ratio - high30Ratio) * 10D) / 10D;

        //创建option对象用于存放年龄占比的具体数据
        Option low20Opt = new Option("20岁以下", low20Ratio);
        Option high20Low30Opt = new Option("20岁到30岁", high20Low30Ratio);
        Option high30Opt = new Option("30岁及30岁以上", high30Ratio);

        //存放年龄占比的list集合
        ArrayList<Option> ageOptions = new ArrayList<>();
        ageOptions.add(low20Opt);
        ageOptions.add(high20Low30Opt);
        ageOptions.add(high30Opt);

        //创建年龄占比的stat对象
        Stat ageStat = new Stat(ageOptions, "用户年龄占比");

        //性别
        TermsAggregation groupby_user_gender = aggregations.getTermsAggregation("groupby_user_gender");
        List<TermsAggregation.Entry> buckets1 = groupby_user_gender.getBuckets();
        Long male = 0L;
        for (TermsAggregation.Entry entry : buckets1) {
            if ("M".equals(entry.getKey())){
                male += entry.getCount();
            }
        }
        //男性占比
        double maleRatio = Math.round(male * 1000D / total) / 10D;
        //女性占比
        double femaleRatio = Math.round((100D - maleRatio) * 10D) / 10D;
        //创建option对象用于存放性别占比的具体数据
        Option maleOpt = new Option("男", maleRatio);
        Option femaleOpt = new Option("女", femaleRatio);

        //创建性别占比的list集合
        ArrayList<Option> genderList = new ArrayList<>();
        genderList.add(maleOpt);
        genderList.add(femaleOpt);
        //创建性别占比的stat对象
        Stat genderStat = new Stat(genderList, "用户性别占比");

        //创建List集合用来存放Stat对象
        ArrayList<Stat> stats = new ArrayList<>();
        stats.add(ageStat);
        stats.add(genderStat);

        //创建Map集合用来存放结果数据
        HashMap<String, Object> result = new HashMap<>();
        result.put("total", total);
        result.put("stat",stats);
        result.put("detail",detail);

        return result;
    }
}

