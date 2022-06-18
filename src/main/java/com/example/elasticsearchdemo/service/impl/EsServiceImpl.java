package com.example.elasticsearchdemo.service.impl;


import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.alibaba.fastjson.JSON;
import com.example.elasticsearchdemo.entity.Goods;
import com.example.elasticsearchdemo.service.EsService;
import com.example.elasticsearchdemo.vo.EsReqVO;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.*;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.ParsedAvg;
import org.elasticsearch.search.builder.PointInTimeBuilder;
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

    @Override
    public void scroll() {
        SearchRequest searchRequest = new SearchRequest("scroll_test");
        SearchSourceBuilder sourceBuilder = SearchSourceBuilder.searchSource();
        sourceBuilder.query(QueryBuilders.matchAllQuery()).size(2);
        searchRequest.source(sourceBuilder).scroll(new Scroll(TimeValue.timeValueMinutes(3)));
        try {
            SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
            String scrollId = response.getScrollId();
            SearchHits hits = response.getHits();
            for (SearchHit hit : hits) {
                System.out.println(hit.getSourceAsString());
            }
            System.out.println("------");
            SearchHits newHists = hits;
            while (newHists.getHits().length > 0) {
                SearchScrollRequest request = new SearchScrollRequest(scrollId);
                request.scroll(new Scroll(TimeValue.timeValueMinutes(3)));
                SearchResponse scrollResponse = client.scroll(request, RequestOptions.DEFAULT);
                scrollId = scrollResponse.getScrollId();
                newHists = scrollResponse.getHits();
                for (SearchHit newHist : newHists) {
                    System.out.println(newHist.getSourceAsString());
                }
                System.out.println("------");
            }
            // 清除scrollId
            ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
            clearScrollRequest.addScrollId(scrollId);
            ClearScrollResponse clearScrollResponse = client.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
            if (clearScrollResponse.isSucceeded()) {
                System.out.println("清除scrollId成功");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void searchAfter() {
        String endPoint = "/kibana_sample_data_flights/_pit?keep_alive=3m";
        Request request = new Request("POST", endPoint);
        String pitId = null;
        try {
            Response pitResponse = client.getLowLevelClient().performRequest(request);
            String body = new String(pitResponse.getEntity().getContent().readAllBytes());
            JSONObject object = JSONUtil.parseObj(body);
            pitId = (String) object.get("id");
        } catch (IOException e) {
            e.printStackTrace();
        }
        // 创建pit
        SearchRequest searchRequest = buildPointInTimeSearch(pitId, null);
        try {
            SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
            SearchHits hits = response.getHits();
            Object[] sortValues = null;
            if (hits.getHits().length != 0) {
                for (SearchHit hit : hits) {
                    System.out.println(hit.getSourceAsString());
                }
                SearchHit lastHit = hits.getAt(hits.getHits().length - 1);
                sortValues = lastHit.getSortValues();
            }
            SearchHits newHits = hits;
            pitId = response.pointInTimeId();
            while (newHits.getHits().length > 0) {
                SearchRequest pitSearchRequest = buildPointInTimeSearch(pitId, sortValues);
                SearchResponse pitResponse = client.search(pitSearchRequest, RequestOptions.DEFAULT);
                SearchHits hits1 = pitResponse.getHits();
                for (SearchHit hit : hits1) {
                    System.out.println(hit.getSourceAsString());
                }
                newHits = hits1;
                pitId = pitResponse.pointInTimeId();
                if (hits1.getHits().length != 0) {
                    SearchHit lastHit = hits1.getAt(hits1.getHits().length - 1);
                    sortValues = lastHit.getSortValues();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private SearchRequest buildPointInTimeSearch(String pid, Object[] searchAfter) {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder sourceBuilder = SearchSourceBuilder.searchSource();
        sourceBuilder.query(QueryBuilders.boolQuery()
                        .must(QueryBuilders.matchQuery("DestCountry", "AU"))
                        .must(QueryBuilders.matchQuery("dayOfWeek", 0)))
                .size(2);
        sourceBuilder.sort("timestamp", SortOrder.DESC);
        sourceBuilder.pointInTimeBuilder(new PointInTimeBuilder(pid).setKeepAlive(TimeValue.timeValueMinutes(3)));
        if (searchAfter != null) {
            sourceBuilder.searchAfter(searchAfter);
        }
        searchRequest.source(sourceBuilder);
        searchRequest.setCcsMinimizeRoundtrips(false);
        return searchRequest;
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
