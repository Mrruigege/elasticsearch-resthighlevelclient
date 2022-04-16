package com.example.elasticsearchdemo;

import com.example.elasticsearchdemo.service.EsAggService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.math.BigDecimal;
import java.util.List;

@SpringBootTest
public class EsAggServiceTest{

    @Autowired
    private EsAggService esAggService;

    @Test
    public void testCardinality() {
        System.out.println(esAggService.cardinalityAttributionArea());
    }

    @Test
    public void testTerms() {
        List<String> areas = esAggService.termsAttributionArea();
        areas.forEach(System.out::println);
    }

    @Test
    public void testRange() {
        System.out.println(esAggService.rangePrice(new BigDecimal(200), new BigDecimal(3000)));
    }

    @Test
    public void testStats() {
        esAggService.statePrice();
    }

    @Test
    public void testExtendedStats() {
        esAggService.extendStatsPrice();
    }
}
