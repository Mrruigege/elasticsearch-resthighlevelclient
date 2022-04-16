package com.example.elasticsearchdemo.service;

import com.example.elasticsearchdemo.entity.Goods;
import com.example.elasticsearchdemo.vo.EsReqVO;

import java.util.List;

public interface EsService {

    void addGoods(Goods goods);

    void batchAddGoods(List<Goods> goodsList);

    void updateGoods(Goods goods);

    List<Goods> queryByMatchAll(EsReqVO reqVO);

    List<Goods> termQuery(EsReqVO reqVO);

    List<Goods> matchQuery(EsReqVO reqVO);

    List<Goods> fuzzyQuery(EsReqVO reqVO);

    List<Goods> wildcardQuery(EsReqVO reqVO);

    List<Goods> boolQuery(EsReqVO reqVO);

    void aggregationQuery(EsReqVO reqVO);
}
