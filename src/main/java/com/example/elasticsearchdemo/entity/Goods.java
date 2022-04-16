package com.example.elasticsearchdemo.entity;

import lombok.Data;

import java.math.BigDecimal;

/**
 * 商品信息
 */
@Data
public class Goods {

    /**
     * 商品id
     */
    private Integer id;

    /**
     * 商品名称
     */
    private String name;

    /**
     *商品价格
     */
    private BigDecimal price;

    /**
     * 商品归属地
     */
    private String attributionArea;
}
