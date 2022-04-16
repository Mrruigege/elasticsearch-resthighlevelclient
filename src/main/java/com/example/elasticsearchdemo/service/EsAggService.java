package com.example.elasticsearchdemo.service;

import java.math.BigDecimal;
import java.util.List;

public interface EsAggService {

    long cardinalityAttributionArea();

    List<String> termsAttributionArea();

    long rangePrice(BigDecimal from, BigDecimal to);

    void statePrice();

    void extendStatsPrice();
}
