package com.example.elasticsearchdemo.service.impl;

import com.example.elasticsearchdemo.service.EsAggService;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.range.BinaryRangeAggregator;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregator;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.Cardinality;
import org.elasticsearch.search.aggregations.metrics.ExtendedStats;
import org.elasticsearch.search.aggregations.metrics.Stats;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * es聚合查询
 */
@Service
public class EsAggServiceImpl implements EsAggService {

    @Autowired
    private RestHighLevelClient client;

    /**
     * 聚合去重，统计个数
     * 根据商品归属地进行去重并统计个数
     */
    @Override
    public long cardinalityAttributionArea() {
        SearchRequest searchRequest = new SearchRequest("goods");
        SearchSourceBuilder sourceBuilder = SearchSourceBuilder.searchSource();
        // 创建聚合查询对象,不止此type为text的，因此这里使用附加的别名keyword
        AggregationBuilder agg = AggregationBuilders.cardinality("distinctAreaCount")
                .field("attributionArea.keyword");
        sourceBuilder.aggregation(agg);
        searchRequest.source(sourceBuilder);
        try {
            SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
            Cardinality cardinality = response.getAggregations().get("distinctAreaCount");
            return cardinality.getValue();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0L;
    }

    /**
     * 聚合分组
     * 统计商品地区
     */
    @Override
    public List<String> termsAttributionArea() {
        List<String> res = new ArrayList<>();
        SearchRequest searchRequest = new SearchRequest("goods");
        SearchSourceBuilder sourceBuilder = SearchSourceBuilder.searchSource();
        // 创建聚合
        sourceBuilder.aggregation(AggregationBuilders
                .terms("distinctAreas")
                .field("attributionArea.keyword"));
        searchRequest.source(sourceBuilder);
        try {
            SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
            Terms terms = response.getAggregations().get("distinctAreas");
            List<? extends Terms.Bucket> buckets = terms.getBuckets();
            for (Terms.Bucket bucket : buckets) {
                res.add(bucket.getKeyAsString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return res;
    }

    /**
     * 聚合范围查询
     * @param from 开始点
     * @param to 结束点
     * @return 个数
     */
    @Override
    public long rangePrice(BigDecimal from, BigDecimal to) {
        SearchRequest searchRequest = new SearchRequest("goods");
        SearchSourceBuilder sourceBuilder = SearchSourceBuilder.searchSource();
        sourceBuilder.aggregation(AggregationBuilders.range("priceRange")
                .field("price")
                .addRange(from.doubleValue(), to.doubleValue()));
        searchRequest.source(sourceBuilder);
        try {
            SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
            Range range =  response.getAggregations().get("priceRange");
            return range.getBuckets().get(0).getDocCount();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0;
    }

    @Override
    public void statePrice() {
        SearchRequest searchRequest = new SearchRequest("goods");
        SearchSourceBuilder sourceBuilder = SearchSourceBuilder.searchSource();
        sourceBuilder.aggregation(AggregationBuilders.stats("priceStats")
                .field("price"));
        searchRequest.source(sourceBuilder);
        try {
            SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
            Stats stats = response.getAggregations().get("priceStats");
            System.out.println(stats.getMax());
            System.out.println(stats.getAvg());
            System.out.println(stats.getMin());
            System.out.println(stats.getSum());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void extendStatsPrice() {
        SearchRequest searchRequest = new SearchRequest("goods");
        SearchSourceBuilder sourceBuilder = SearchSourceBuilder.searchSource();
        sourceBuilder.aggregation(AggregationBuilders.extendedStats("exPrice")
                .field("price"));
        searchRequest.source(sourceBuilder);
        try {
            SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
            ExtendedStats exPrice = response.getAggregations().get("exPrice");
            System.out.println(exPrice.getStdDeviation());
            System.out.println(exPrice.getVariancePopulation());
            System.out.println(exPrice.getStdDeviation());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
