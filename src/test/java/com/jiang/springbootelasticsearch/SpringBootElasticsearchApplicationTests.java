package com.jiang.springbootelasticsearch;

import co.elastic.clients.elasticsearch._types.aggregations.AggregateBuilders;
import com.alibaba.fastjson.JSON;
import com.jiang.springbootelasticsearch.model.User;
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
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xcontent.XContentType;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.web.servlet.resource.ResourceUrlProvider;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

/**
 * 高级API 在7.15被废弃
 */
@SpringBootTest
class SpringBootElasticsearchApplicationTests {

    public static final String INDEX="test4";

    @Resource
    private RestHighLevelClient client;
    @Autowired
    private ResourceUrlProvider mvcResourceUrlProvider;

    /**
     * 创建索引
     * @throws IOException
     */
    @Test
    void testCreateIndex() throws IOException {

        //创建心的索引：
        CreateIndexRequest indexRequest = new CreateIndexRequest(INDEX);
        //连接请求：
        CreateIndexResponse Response = client.indices().create(indexRequest, RequestOptions.DEFAULT);
        System.out.println(Response);

    }

    /**
     * 测试索引是否存在
     * @throws IOException
     */
    @Test
    void  testExistsIndex() throws IOException {

        GetIndexRequest getIndexRequest = new GetIndexRequest(INDEX);
        boolean exists = client.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    /**
     * 测试是否删除索引：
     * @throws IOException
     */
    @Test
    void deleteIndex() throws IOException {
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("user");
        AcknowledgedResponse response = client.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
        System.out.println(response.isAcknowledged());
    }

    /**
     * 新增文档
     * @throws IOException
     */
    @Test
    void testAddDocument() throws IOException {

        User user = new User("jiangyanming", 25);
        //新增一个文档：
        IndexRequest request = new IndexRequest(INDEX);
        //设置id
        request.id(String.valueOf(1));
        String jsonString = JSON.toJSONString(user);
        //转为json
        IndexRequest source = request.source(jsonString, XContentType.JSON);
        //传入到客户端：
        IndexResponse response = client.index(source, RequestOptions.DEFAULT);

        System.out.println(response.getId()); //随机的id
        System.out.println(response.getIndex()); // 索引名字
        System.out.println(response.getResult()); //索引创建的结果：created

    }

    /**
     * 更新文档
     * @throws IOException
     */
    @Test
    void testUpdateDocument() throws IOException {
        UpdateRequest updateRequest = new UpdateRequest();
        //更新的索引和下标
        UpdateRequest request= updateRequest.index(INDEX).id("1");
        //更新的内容
        request.doc(XContentType.JSON,"userName","xiaofan","age",30);
        //更新操作
        UpdateResponse response = client.update(request, RequestOptions.DEFAULT);
        System.out.println(response.getId());
        System.out.println(response.getIndex());
        System.out.println(response.getResult());
    }

    /**
     * 查询文档
     * @throws IOException
     */
    @Test
    void getIndexDocument() throws IOException {
        //得到索引请求
        GetRequest request = new GetRequest(INDEX,"1");
        //客户端发起请求：
        GetResponse response = client.get(request, RequestOptions.DEFAULT);
        System.out.println(response.getId());
        System.out.println(response.getIndex());
        System.out.println(response.getSource());


    }

    /**
     * 删除文档
     * @throws IOException
     */
    @Test
    void testDeleteDocument() throws IOException {

        //删除文档
        DeleteRequest deleteRequest = new DeleteRequest(INDEX, "1");
        DeleteResponse response = client.delete(deleteRequest, RequestOptions.DEFAULT);
        System.out.println(response.getResult());
        System.out.println(response.toString());
    }


    /**
     * 批量新增
     */
    @Test
    void bulkAddDocument() throws IOException {
        BulkRequest bulkRequest = new BulkRequest();

        ArrayList<User> userList = new ArrayList<>();
        User user1 = new User("小白", 20);
        User user2 = new User("小黑", 30);
        User user3 = new User("消息", 25);
        userList.add(user1);
        userList.add(user2);
        userList.add(user3);

        for (int i = 0; i < userList.size(); i++) {
            //遍历每一条数据
            bulkRequest.add(new IndexRequest(INDEX).id(String.valueOf(i+1)).source(JSON.toJSONString(userList.get(i)), XContentType.JSON));
        }
        BulkResponse response = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println(response.getTook());
        System.out.println(response.getItems());
    }


    /**
     * 批量删除
     */
    @Test
    void bulkDeleteDocument() throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(new DeleteRequest().index(INDEX).id("2"));
        bulkRequest.add(new DeleteRequest().index(INDEX).id("3"));

        BulkResponse response = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println(response.getTook());
        System.out.println(response.getItems());
    }

    /**
     * 查询所有数据
     * @throws IOException
     */
    @Test
    void testSearchDocument() throws IOException {
        SearchRequest request = new SearchRequest();
        request.indices(INDEX);
        //查询请求体
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //查询所有数据
        sourceBuilder.query(QueryBuilders.matchAllQuery());
        //放入数据
        request.source(sourceBuilder);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        System.out.println(response.getTook());
        System.out.println(response.getHits());

        //打印数据
        SearchHits hits = response.getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
        System.out.println(response.getAggregations());
    }

    /**
     * 条件查询
     * @throws IOException
     */
    @Test
    void testTermDocument() throws IOException {
        SearchRequest request = new SearchRequest();
        request.indices(INDEX);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //条件查询
        searchSourceBuilder.query(QueryBuilders.termQuery("age", 25));
        request.source(searchSourceBuilder);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        SearchHits hits = response.getHits();
        System.out.println(response.getTook());
        System.out.println(hits.getHits());
        System.out.println(hits.getTotalHits());
        System.out.println("====================>");
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString()); //输出结果
        }
        System.out.println("<=====================");
    }

    /**
     * 分页查询
     * @throws IOException
     */
    @Test
    void testRenyeDocument() throws IOException {

        SearchRequest request = new SearchRequest();
        request.indices(INDEX);
        //查询请求：
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        //分页条件
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(4);
        request.source(searchSourceBuilder);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);


        SearchHits hits = response.getHits();
        System.out.println(response.getTook());
        System.out.println(hits.getHits());
        System.out.println(hits.getTotalHits());
        System.out.println("====================>");
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString()); //输出结果
        }
        System.out.println("<=====================");

    }

    /**
     * 排序查询
     * @throws IOException
     */
    @Test
    void testSortDocument() throws IOException {
        SearchRequest request = new SearchRequest();
        request.indices(INDEX);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchSourceBuilder.sort("age", SortOrder.DESC);//降序
        request.source(searchSourceBuilder);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        SearchHits hits = response.getHits();
        System.out.println(response.getTook());
        System.out.println(hits.getHits());
        System.out.println(hits.getTotalHits());
        System.out.println("====================>");
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString()); //输出结果
        }
        System.out.println("<=====================");
    }

    /**
     * 组合查询
     * @throws IOException
     */
    @Test
    void testQueryDocument() throws IOException {
        SearchRequest request = new SearchRequest();
        request.indices(INDEX);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery(); //布尔查询
        boolQueryBuilder.must(QueryBuilders.termQuery("age", 25));
        //一定不包含
        boolQueryBuilder.mustNot(QueryBuilders.termQuery("userName", "jiangyanming"));

        searchSourceBuilder.query(boolQueryBuilder);
        request.source(searchSourceBuilder);

        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        SearchHits hits = response.getHits();
        System.out.println(response.getTook());
        System.out.println(hits.getHits());
        System.out.println(hits.getTotalHits());
        System.out.println("====================>");
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString()); //输出结果
        }
        System.out.println("<=====================");
    }

    /**
     * 范围查询
     * @throws IOException
     */
    @Test
    void testRangeDocument() throws IOException {

        SearchRequest request = new SearchRequest().indices(INDEX);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("age");
        rangeQueryBuilder.gt(20);
        searchSourceBuilder.query(rangeQueryBuilder); //查询条件
        request.source(searchSourceBuilder);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        SearchHits hits = response.getHits();
        System.out.println(response.getTook());
        System.out.println(hits.getHits());
        System.out.println(hits.getTotalHits());
        System.out.println("====================>");
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString()); //输出结果
        }
        System.out.println("<=====================");
    }

    /**
     * 模糊查询
     * @throws IOException
     */
    @Test
    void testFuzzyDocument() throws IOException {
        SearchRequest request = new SearchRequest();
        request.indices(INDEX);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //模糊查询
        searchSourceBuilder.query(QueryBuilders.fuzzyQuery("userName","小").fuzziness(Fuzziness.ONE));
        request.source(searchSourceBuilder);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        SearchHits hits = response.getHits();
        System.out.println(response.getTook());
        System.out.println(hits.getHits());
        System.out.println(hits.getTotalHits());
        System.out.println("====================>");
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString()); //输出结果
        }
        System.out.println("<=====================");
    }

    /**
     * 高亮查询：
     * @throws IOException
     */
    @Test
    void testHighlightDocument() throws IOException {
        SearchRequest request = new SearchRequest();
        request.indices(INDEX);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //查询条件
        searchSourceBuilder.query(QueryBuilders.matchQuery("userName","小白"));
        //设置高亮
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.preTags("<font color='red'>");//设置标签前缀
        highlightBuilder.postTags("</font>");//设置标签后缀
        highlightBuilder.field("userName");//设置高亮字段
        searchSourceBuilder.highlighter(highlightBuilder);
        request.source(searchSourceBuilder);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        //返回结果：
        SearchHits hits = response.getHits();
        System.out.println(response.getTook());
        System.out.println(hits.getHits());
        System.out.println(hits.getTotalHits());
        System.out.println("====================>");
        for (SearchHit hit : hits) {
            //输出结果
            System.out.println(hit.getSourceAsString());
            //打印高亮结果：
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            System.out.println(highlightFields);
        }
        System.out.println("<=====================");
    }

    /**
     * 最大值查询
     * @throws IOException
     */
    @Test
    void testMaxValueDocument() throws IOException{
        SearchRequest request = new SearchRequest().indices(INDEX);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        AggregationBuilder aggregationBuilder = AggregationBuilders.max("maxAge").field("age");
        searchSourceBuilder.aggregation(aggregationBuilder);
        request.source(searchSourceBuilder);

        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        System.out.println("====================>");
        System.out.println(response);
        System.out.println("<=====================");

    }

    /**
     * 分组查询
     * @throws IOException
     */
    @Test
    void testGroupByDocument() throws IOException {
        SearchRequest request = new SearchRequest().indices(INDEX);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //分组,分组名字叫age_group
        TermsAggregationBuilder field = AggregationBuilders.terms("age_groupby").field("age");
        searchSourceBuilder.aggregation(field);
        request.source(searchSourceBuilder);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        System.out.println("====================>");
        System.out.println(response);
        System.out.println("<=====================");
    }

}
