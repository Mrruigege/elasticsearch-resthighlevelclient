package com.example.elasticsearchdemo;

import com.example.elasticsearchdemo.entity.Goods;
import com.example.elasticsearchdemo.service.EsService;
import com.example.elasticsearchdemo.vo.EsReqVO;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

@SpringBootTest
public class EsServiceTest {

    @Autowired
    private EsService esService;

    @Test
    public void testAdd() {
        Goods goods = new Goods();
        goods.setId(11);
        goods.setName("oppo reno 6 pro");
        goods.setPrice(BigDecimal.valueOf(2800));
        goods.setAttributionArea("深圳前海");
        esService.addGoods(goods);
    }

    @Test
    public void testBatchAdd() {
        List<Goods> list = new ArrayList<>();
        for (int i = 4; i < 12; i++) {
            Goods goods = new Goods();
            goods.setId(i);
            goods.setName("oppo reno" + i);
            goods.setPrice(BigDecimal.valueOf(2000 + i * 100));
            goods.setAttributionArea("深圳前海");
            list.add(goods);
        }
        esService.batchAddGoods(list);
    }

    @Test
    public void update() {
        Goods goods = new Goods();
        goods.setId(1);
        goods.setName("oppo find x5");
        goods.setPrice(BigDecimal.valueOf(6499.11));
        goods.setAttributionArea("深圳保安");
        esService.updateGoods(goods);
    }

    @Test
    public void queryByMatchAll() {
        EsReqVO reqVO = new EsReqVO();
        List<Goods> goods = esService.queryByMatchAll(reqVO);
        for (Goods g : goods) {
            System.out.println(g);
        }
    }

    @Test
    public void testTermQuery() {
        EsReqVO esReqVO = new EsReqVO();
        List<Goods> goods = esService.termQuery(esReqVO);
        for (Goods g : goods) {
            System.out.println(g);
        }
    }

    @Test
    public void testMatchQuery() {
        EsReqVO esReqVO = new EsReqVO();
        List<Goods> goods = esService.matchQuery(esReqVO);
        for (Goods g : goods) {
            System.out.println(g);
        }
    }

    @Test
    public void testBoolQuery() {
        EsReqVO reqVO = new EsReqVO();
        reqVO.setName("find");
        reqVO.setAttributionArea("保安");
        List<Goods> goods = esService.boolQuery(reqVO);
        for (Goods g : goods) {
            System.out.println(g);
        }
    }

    @Test
    public void testFuzzyQuery() {
        EsReqVO esReqVO = new EsReqVO();
        esReqVO.setName("邓紫");
        List<Goods> goods = esService.fuzzyQuery(esReqVO);
        goods.forEach(System.out::println);
    }

    @Test
    public void testWildCardQuery() {
        EsReqVO reqVO = new EsReqVO();
        reqVO.setName("find");
        List<Goods> goods = esService.wildcardQuery(reqVO);
        goods.forEach(System.out:: println);
    }

    @Test
    public void testAggregationQuery() {
        EsReqVO reqVO = new EsReqVO();
        esService.aggregationQuery(reqVO);
    }
}
