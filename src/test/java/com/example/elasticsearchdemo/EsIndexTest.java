package com.example.elasticsearchdemo;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.Map;

@SpringBootTest
public class EsIndexTest {

    @Autowired
    private RestHighLevelClient client;

    /**
     * 添加索引
     */
    @Test
    public void addIndex() throws IOException {
        // 获取索引对象
        IndicesClient indices = client.indices();
        // 具体操作
        CreateIndexRequest createRequest = new CreateIndexRequest("es_demo");
        CreateIndexResponse indexResponse = indices.create(createRequest, RequestOptions.DEFAULT);
        System.out.println(indexResponse.isAcknowledged());
    }

    /**
     * 添加索引和mapping
     * @throws IOException 创建异常
     */
    @Test
    public void addIndexAndMapping() throws IOException {
        // 获取索引对象
        IndicesClient indices = client.indices();
        // 具体操作
        CreateIndexRequest createRequest = new CreateIndexRequest("es_demo");
        String mapping = "{\n" +
                "      \"properties\" : {\n" +
                "        \"address\" : {\n" +
                "          \"type\" : \"text\",\n" +
                "          \"fields\" : {\n" +
                "            \"keyword\" : {\n" +
                "              \"type\" : \"keyword\"\n" +
                "            }\n" +
                "          },\n" +
                "          \"analyzer\" : \"ik_max_word\"\n" +
                "        },\n" +
                "        \"name\" : {\n" +
                "          \"type\" : \"keyword\"\n" +
                "        }\n" +
                "      }\n" +
                "    }";
        createRequest.mapping(mapping, XContentType.JSON);
        CreateIndexResponse indexResponse = indices.create(createRequest, RequestOptions.DEFAULT);
        System.out.println(indexResponse.isAcknowledged());
    }

    /**
     * 获取es索引
     * @throws IOException 获取时发生的异常
     */
    @Test
    public void queryIndex() throws IOException {
        IndicesClient indices = client.indices();
        GetIndexRequest request = new GetIndexRequest("es_demo");
        GetIndexResponse response = indices.get(request, RequestOptions.DEFAULT);
        Map<String, MappingMetadata> mappings = response.getMappings();
        for (String key : mappings.keySet()) {
            System.out.println(key + ":" + mappings.get(key).sourceAsMap());
        }
    }

    /**
     * 删除索引
     */
    @Test
    public void deleteIndex() throws IOException {
        IndicesClient indices = client.indices();
        DeleteIndexRequest deleteRequest = new DeleteIndexRequest("es_demo");
        AcknowledgedResponse response = indices.delete(deleteRequest, RequestOptions.DEFAULT);
        System.out.println(response.isAcknowledged());
    }

    /**
     * 判断索引是否存在
     */
    @Test
    public void existIndex() throws IOException {
        IndicesClient indices = client.indices();
        GetIndexRequest request = new GetIndexRequest("es_demo");
        boolean exists = indices.exists(request, RequestOptions.DEFAULT);
        System.out.println(exists);
    }
}
