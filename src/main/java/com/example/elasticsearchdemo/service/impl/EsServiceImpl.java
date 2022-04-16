package com.example.elasticsearchdemo.service.impl;

import com.alibaba.fastjson.JSON;
import com.example.elasticsearchdemo.entity.Goods;
import com.example.elasticsearchdemo.service.EsService;
import com.example.elasticsearchdemo.vo.EsReqVO;
import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.FuzzyQuery;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.ParsedAvg;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * es服务方法
 */
@Slf4j
@Service
public class EsServiceImpl implements EsService {

    @Autowired
    private RestHighLevelClient client;

    /**
     * 添加单个文档
     * @param goods
     */
    @Override
    public void addGoods(Goods goods) {
        IndexRequest request = new IndexRequest("goods");
        request.id(String.valueOf(goods.getId()));
        request.source(JSON.toJSONString(goods), XContentType.JSON);
        try {
            IndexResponse response = client.index(request, RequestOptions.DEFAULT);
            System.out.println(response);
        } catch (IOException e) {
            log.error("add goods to es error!");
            e.printStackTrace();
        }
    }

    /**
     * 批量添加文档
     * @param goodsList
     */
    @Override
    public void batchAddGoods(List<Goods> goodsList) {
        if (!CollectionUtils.isEmpty(goodsList)) {
            BulkRequest bulkRequest = new BulkRequest();
            for (Goods goods : goodsList) {
                IndexRequest request = new IndexRequest("goods");
                request.id(String.valueOf(goods.getId()));
                request.source(JSON.toJSONString(goods), XContentType.JSON);
                bulkRequest.add(request);
            }
            try {
                client.bulk(bulkRequest, RequestOptions.DEFAULT);
            } catch (IOException e) {
                log.error("batch add goods to es error!");
                e.printStackTrace();
            }
        }
    }

    /**
     * 更新文档
     * @param goods
     */
    @Override
    public void updateGoods(Goods goods) {
        UpdateRequest request = new UpdateRequest();
        request.index("goods");
        request.id(String.valueOf(goods.getId()));
        request.doc(JSON.toJSONString(goods), XContentType.JSON);
        try {
            client.update(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.error("update goods to es error!");
            e.printStackTrace();
        }
    }

    /**
     * 通过matchAll查询文档
     * @return
     */
    @Override
    public List<Goods> queryByMatchAll(EsReqVO reqVO) {
        SearchRequest searchRequest = new SearchRequest("goods");
        SearchSourceBuilder builder = new SearchSourceBuilder();
        QueryBuilder queryBuilder = QueryBuilders.matchAllQuery();
        builder.query(queryBuilder);
        builder.from(reqVO.getPageNo() * reqVO.getPageSize());
        builder.size(reqVO.getPageSize());
        // 按照价格降序排序
        builder.sort("price", SortOrder.DESC);
        // 指定searchBuilder
        searchRequest.source(builder);
        List<Goods> res = new ArrayList<>();
        try {
            SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
            SearchHit[] hits = response.getHits().getHits();
            for (SearchHit hit : hits) {
                Goods goods = JSON.parseObject(hit.getSourceAsString(), Goods.class);
                res.add(goods);
            }
        } catch (IOException e) {
            log.error("query by matchAll error");
            e.printStackTrace();
        }
        return res;
    }

    @Override
    public List<Goods> termQuery(EsReqVO reqVO) {
        SearchRequest searchRequest = new SearchRequest("goods");
        // 创建searchSourceBuilder对象
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        // 创建具体的queryBuilder对象
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name", "find");
        // 设置属性queryBuilder
        sourceBuilder.query(termQueryBuilder);
        // 设置分页
        sourceBuilder.from(reqVO.getPageNo() * reqVO.getPageSize());
        sourceBuilder.size(reqVO.getPageSize());
        // 设置searchSourceBuilder
        searchRequest.source(sourceBuilder);
        try {
            SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
            return getResult(response);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 模糊查询
     */
    @Override
    public List<Goods> fuzzyQuery(EsReqVO reqVO) {
        SearchRequest searchRequest = new SearchRequest("goods");
        SearchSourceBuilder sourceBuilder = SearchSourceBuilder.searchSource()
                .query(QueryBuilders.fuzzyQuery("name",  reqVO.getName()))
                .from(reqVO.getPageNo() * reqVO.getPageSize())
                .size(reqVO.getPageSize());
        searchRequest.source(sourceBuilder);
        try {
            SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
            return getResult(response);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public List<Goods> matchQuery(EsReqVO reqVO) {
        SearchRequest searchRequest = new SearchRequest("goods");
        SearchSourceBuilder sourceBuilder = SearchSourceBuilder.searchSource()
                .query(QueryBuilders.matchQuery("name", "find reno"))
                .from(reqVO.getPageNo() * reqVO.getPageNo())
                .size(reqVO.getPageSize());
        searchRequest.source(sourceBuilder);
        try {
            SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
            return getResult(response);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 多条件查询
     */
    @Override
    public List<Goods> boolQuery(EsReqVO reqVO) {
        SearchRequest searchRequest = new SearchRequest("goods");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        if (!StringUtils.isEmpty(reqVO.getAttributionArea())) {
            queryBuilder.filter(QueryBuilders.wildcardQuery("attributionArea","*" +
                    reqVO.getAttributionArea() + "*"));
        }
        if (!StringUtils.isEmpty(reqVO.getName())) {
            queryBuilder.filter(QueryBuilders.wildcardQuery("name", "*" + reqVO.getName()
            + "*"));
        }
        if (reqVO.getPrice() != null) {
            queryBuilder.filter(QueryBuilders.rangeQuery("price").lte(reqVO.getMaxPrice()).gte(reqVO.getMinPrice()));
        }
        sourceBuilder.query(queryBuilder);
        searchRequest.source(sourceBuilder);
        try {
            SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
            return getResult(response);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 模糊查询第二种方式
     */
    @Override
    public List<Goods> wildcardQuery(EsReqVO reqVO) {
        SearchRequest searchRequest = new SearchRequest("goods");
        SearchSourceBuilder sourceBuilder = SearchSourceBuilder.searchSource()
                .query(QueryBuilders.wildcardQuery("name", "*" + reqVO.getName() +
                        "*"))
                .from(reqVO.getPageNo() * reqVO.getPageSize())
                .size(reqVO.getPageSize());
        searchRequest.source(sourceBuilder);
        try {
            SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
            return getResult(response);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void aggregationQuery(EsReqVO reqVO) {
        SearchRequest searchRequest = new SearchRequest("goods");
        SearchSourceBuilder sourceBuilder = SearchSourceBuilder.searchSource();
        AggregationBuilder aggregationBuilder = AggregationBuilders.avg("avg_price")
                .field("price");
        sourceBuilder.aggregation(aggregationBuilder);
        searchRequest.source(sourceBuilder);
        try {
            SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
            ParsedAvg avgPrice = response.getAggregations().get("avg_price");
            System.out.println(avgPrice.getValue());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public List<Goods> getResult(SearchResponse response) {
        List<Goods> res = new ArrayList<>();
        SearchHit[] hits = response.getHits().getHits();
        if (hits.length != 0) {
            for (SearchHit hit : hits) {
                Goods goods = JSON.parseObject(hit.getSourceAsString(), Goods.class);
                res.add(goods);
            }
        }
        return res;
    }
}
