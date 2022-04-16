package com.example.elasticsearchdemo;

import com.alibaba.fastjson.JSON;
import com.example.elasticsearchdemo.entity.Person;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@SpringBootTest
public class EsDocTest {

    @Autowired
    private RestHighLevelClient client;

    /**
     * 添加文档 map方式
     */
    @Test
    public void addDoc() throws IOException {
        Map<String, String> data = new HashMap<>();
        data.put("name", "mike");
        data.put("address", "深圳保安");
        IndexRequest indexRequest = new IndexRequest("person").source(data);
        IndexResponse response = client.index(indexRequest, RequestOptions.DEFAULT);
        System.out.println(response.getId());
    }

    /**
     * 添加文档，对象方式
     */
    @Test
    public void addDoc1() throws IOException {
        Person person = new Person();
        person.setName("张强");
        person.setAddress("重庆永川");
        IndexRequest request = new IndexRequest("person").source(JSON.toJSONString(person), XContentType.JSON);
        IndexResponse index = client.index(request, RequestOptions.DEFAULT);
        System.out.println(index.getId());
    }
}