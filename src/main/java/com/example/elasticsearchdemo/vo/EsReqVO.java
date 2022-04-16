package com.example.elasticsearchdemo.vo;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class EsReqVO {

    private int pageNo = 0;

    private int pageSize = 10;

    private int id;

    private String name;

    private BigDecimal price;

    private String maxPrice;

    private String minPrice;

    private String attributionArea;
}
