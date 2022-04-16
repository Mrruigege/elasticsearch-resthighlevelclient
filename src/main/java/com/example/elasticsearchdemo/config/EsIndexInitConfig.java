package com.example.elasticsearchdemo.config;

import com.alibaba.fastjson.JSON;
import com.example.elasticsearchdemo.entity.Goods;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;

@Slf4j
@Configuration
public class EsIndexInitConfig implements ApplicationRunner {

    @Autowired
    private RestHighLevelClient client;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        initGoodsIndex();
    }

    public void initGoodsIndex() {
        IndicesClient indices = client.indices();
        GetIndexRequest request = new GetIndexRequest("goods");
        try {
            boolean exists = indices.exists(request, RequestOptions.DEFAULT);
            if (exists) {
                // 如果存在，则放弃创建索引
                log.info("es goods index exist!");
                return;
            }
            CreateIndexRequest createIndexRequest = new CreateIndexRequest("goods");
            String mapping = "{\n" +
                    "    \"properties\": {\n" +
                    "      \"id\": {\n" +
                    "        \"type\": \"keyword\"\n" +
                    "      },\n" +
                    "      \"name\": {\n" +
                    "        \"type\": \"text\",\n" +
                    "        \"analyzer\": \"ik_max_word\"\n" +
                    "      },\n" +
                    "      \"price\": {\n" +
                    "        \"type\": \"double\"\n" +
                    "      },\n" +
                    "      \"attributionArea\": {\n" +
                    "        \"type\": \"text\",\n" +
                    "        \"analyzer\": \"ik_max_word\"\n" +
                    "      }\n" +
                    "    }\n" +
                    "  }";
            createIndexRequest.mapping(mapping, XContentType.JSON);
            CreateIndexResponse response = indices.create(createIndexRequest, RequestOptions.DEFAULT);
            if (response.isAcknowledged()) {
                log.info("create goods index success!");
            } else {
                log.error("create goods index error!");
            }
        } catch (IOException e) {
            log.error("es goods index created error!");
            e.printStackTrace();
        }
    }
}
