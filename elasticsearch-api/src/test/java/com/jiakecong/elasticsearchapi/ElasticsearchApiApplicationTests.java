package com.jiakecong.elasticsearchapi;

import com.alibaba.fastjson.JSON;
import com.jiakecong.elasticsearchapi.pojo.User;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

@SpringBootTest
class ElasticsearchApiApplicationTests {

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    @Test
    void contextLoads() {
    }

    @Test
    void testCreateIndex() throws IOException {
        // 1.创建索引请求
        CreateIndexRequest createIndexRequset = new CreateIndexRequest("jiakecong");
        //2. 客户端执行请求 IndicesClient, 请求后获取响应
        CreateIndexResponse createIndexResponse = restHighLevelClient.indices().create(createIndexRequset, RequestOptions.DEFAULT);

        System.out.println(createIndexRequset);
    }

    @Test
    void testExistIndex() throws IOException {
        GetIndexRequest jiakecong = new GetIndexRequest("jiakecong");
        boolean exists = restHighLevelClient.indices().exists(jiakecong, RequestOptions.DEFAULT);

        System.out.println(exists);
    }

    @Test
    void testDeleteIndex() throws IOException {
        DeleteIndexRequest jiakecong = new DeleteIndexRequest("jiakecong");
        AcknowledgedResponse delete = restHighLevelClient.indices().delete(jiakecong, RequestOptions.DEFAULT);
        System.out.println(delete.isAcknowledged());
    }


    @Test
    void testAddDocument() throws IOException {
        User user = new User("jiakecong", 23);

//        put /jiakecong/_doc/1 {...}
        IndexRequest jiakecong = new IndexRequest("jiakecong");
        jiakecong.id("1");
        jiakecong.timeout(TimeValue.timeValueSeconds(1));
        jiakecong.source(JSON.toJSONString(user), XContentType.JSON);

        IndexResponse index = restHighLevelClient.index(jiakecong, RequestOptions.DEFAULT);
        System.out.println(index.toString());
        System.out.println(index.status());

    }

    @Test
    void testIsExists() throws IOException {
        GetRequest getIndexRequest = new GetRequest("jiakecong", "1");

//        不获取返回的 _source的上下文了
        getIndexRequest.fetchSourceContext(new FetchSourceContext(false));
        getIndexRequest.storedFields("_none_");

        boolean exists = restHighLevelClient.exists(getIndexRequest, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    @Test
    void testGetDoucment() throws IOException {
        GetRequest jiakecong = new GetRequest("jiakecong", "1");
        GetResponse documentFields = restHighLevelClient.get(jiakecong, RequestOptions.DEFAULT);

        System.out.println(documentFields.getSource());
        System.out.println(documentFields);
    }


    @Test
    void testupdateRequest() throws IOException {
        UpdateRequest updateRequest = new UpdateRequest("jiakecong", "1");

        updateRequest.timeout("1s");

        User user = new User("jiakecong", 25);

        updateRequest.doc(JSON.toJSONString(user), XContentType.JSON);

        UpdateResponse update = restHighLevelClient.update(updateRequest, RequestOptions.DEFAULT);

        System.out.println(update);

    }

    @Test
    void testDeleteRequest() throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest("jiakecong", "1");

        deleteRequest.timeout("1s");

        DeleteResponse deleteResponse = restHighLevelClient.delete(deleteRequest, RequestOptions.DEFAULT);

        System.out.println(deleteResponse.status());

    }

    @Test
    void testBulkRequest() throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.timeout("10s");

        ArrayList<User> userList = new ArrayList<>();
        userList.add(new User("jiakecong", 21));
        userList.add(new User("jiakecong", 22));
        userList.add(new User("jiakecong", 23));
        userList.add(new User("jiakecong", 24));
        userList.add(new User("jiakecong", 25));
        userList.add(new User("jiakecong", 26));
        userList.add(new User("jiakecong", 27));

        for (int i = 0; i < userList.size(); i++) {
//            批量更新和批量刪除同理，在这里进行处理
            bulkRequest.add(new IndexRequest("jiakecong")
                    .id("" + ( i + 1))
            .source(JSON.toJSONString(userList.get(i)), XContentType.JSON));
        }

        BulkResponse bulkResponse = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);

        System.out.println(bulkResponse.hasFailures());

    }
    
    @Test
    void testSearch() throws IOException {
        SearchRequest searchRequest = new SearchRequest("jiakecong");
        
//        构建搜索条件
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
//        查询条件 可以使用QueryBuilders 工具来实现
//        QueryBuilders.termQuery() 精确查询
//        QueryBuidlers.matchAllQuery() 匹配所有
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name", "jiakecong");

        searchSourceBuilder.query(termQueryBuilder);
        searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));

        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

        System.out.println(JSON.toJSONString(searchResponse.getHits()));
        System.out.println("============================");
        for (SearchHit documentFields: searchResponse.getHits().getHits()) {
            System.out.println(JSON.toJSONString(documentFields.getSourceAsMap()));
            System.out.println(JSON.toJSONString(documentFields.getSourceAsString()));
        }



    }


}
