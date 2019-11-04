package com.es.demo.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.es.demo.utils.ESUtil;
import com.es.demo.vo.ResponseBean;
import com.es.demo.vo.User;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.*;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedLongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortMode;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @Author: qjc
 * @Date: 2019/09/18
 */
@Api(value = "ES测试接口", tags = {"ES测试接口"})
@RestController
@RequestMapping("/es")
@CrossOrigin(origins = "*", methods = {RequestMethod.GET, RequestMethod.POST, RequestMethod.DELETE, RequestMethod.PUT})
@Slf4j
public class ESTestController {

    @Resource
    private RestHighLevelClient restHighLevelClient;
    @Resource
    ESUtil esUtil;

    @ApiOperation(value = "es测试创建索引接口", notes = "es测试创建索引接口")
    @RequestMapping(value = "/index/create", method = RequestMethod.POST)
    public ResponseBean createIndex(@RequestParam String indexName) {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder()
                    .startObject()
                    .field("properties")
                    .startObject()
                    .field("name").startObject().field("index", "true").field("type", "keyword").endObject()
                    .field("age").startObject().field("index", "true").field("type", "integer").endObject()
                    .field("money").startObject().field("index", "true").field("type", "double").endObject()
                    .field("address").startObject().field("index", "true").field("type", "text").field("analyzer", "ik_max_word").endObject()
                    .field("birthday").startObject().field("index", "true").field("type", "date").field("format", "strict_date_optional_time||epoch_millis").endObject()
                    .endObject()
                    .endObject();
            CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
            createIndexRequest.mapping(builder);
            CreateIndexResponse createIndexResponse = restHighLevelClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
            boolean acknowledged = createIndexResponse.isAcknowledged();
            if (acknowledged) {
                return new ResponseBean(200, "创建成功", null);
            } else {
                return new ResponseBean(1002, "创建失败", null);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @ApiOperation(value = "es测试是否存在索引接口", notes = "es测试是否存在索引接口")
    @RequestMapping(value = "/index/exists", method = RequestMethod.POST)
    public ResponseBean indexExists(@RequestParam String indexName) {
        boolean isExists = esUtil.isIndexExists(indexName);
        return new ResponseBean(200, "查询成功", isExists);
    }

    @ApiOperation(value = "es测试删除索引接口", notes = "es测试删除索引接口")
    @RequestMapping(value = "/index/delete", method = RequestMethod.POST)
    public ResponseBean deleteIndex(@RequestParam String indexName) {
        boolean isDelete = esUtil.deleteIndex(indexName);
        if (isDelete) {
            return new ResponseBean(200, "删除成功", null);
        } else {
            return new ResponseBean(10002, "删除失败", null);
        }
    }

    @ApiOperation(value = "es测试插入接口", notes = "es测试插入接口")
    @RequestMapping(value = "/insert/data", method = RequestMethod.POST)
    public ResponseBean findIndustryClassList(@RequestBody User user, @RequestParam String indexName) {
        IndexRequest indexRequest = new IndexRequest(indexName);
        String userJson = JSONObject.toJSONString(user);
        indexRequest.source(userJson, XContentType.JSON);
        try {
            IndexResponse indexResponse = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
            if (indexResponse != null) {
                String id = indexResponse.getId();
                String index = indexResponse.getIndex();
                long version = indexResponse.getVersion();
                log.info("index:{},id:{}", index, id);
                if (indexResponse.getResult() == DocWriteResponse.Result.CREATED) {
                    System.out.println("新增文档成功!" + index + "-" + id + "-" + version);
                    return new ResponseBean(200, "插入成功", id);
                } else if (indexResponse.getResult() == DocWriteResponse.Result.UPDATED) {
                    System.out.println("修改文档成功!");
                    return new ResponseBean(10001, "插入失败", null);
                }
                // 分片处理信息
                ReplicationResponse.ShardInfo shardInfo = indexResponse.getShardInfo();
                if (shardInfo.getTotal() != shardInfo.getSuccessful()) {
                    System.out.println("分片处理信息.....");
                }
                // 如果有分片副本失败，可以获得失败原因信息
                if (shardInfo.getFailed() > 0) {
                    for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
                        String reason = failure.reason();
                        System.out.println("副本失败原因：" + reason);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @ApiOperation(value = "es测试批量插入接口", notes = "es测试批量插入接口")
    @RequestMapping(value = "/insert/data/bulk", method = RequestMethod.POST)
    public ResponseBean insertBulkData(@RequestParam String indexName) {
        BulkRequest bulkRequest = new BulkRequest();
        User user = new User();
        for (int i = 1; i <= 11000; i++) {
            user.setName("吴六" + i);
            user.setAddress("北京" + i);
            user.setAge(i);
            user.setMoney(new Double(i));
            user.setBirthday("2019-11-05");

            String userJson = JSONObject.toJSONString(user);
            IndexRequest indexRequest = new IndexRequest(indexName);
            indexRequest.source(userJson, XContentType.JSON);
            bulkRequest.add(indexRequest);
        }
        try {
            BulkResponse bulkResponse = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
            RestStatus restStatus = bulkResponse.status();
            int status = restStatus.getStatus();
            if (status == 200) {
                return new ResponseBean(status, "插入成功", null);
            } else {
                return new ResponseBean(status, "插入失败", null);

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }


    @ApiOperation(value = "es测试普通查询接口", notes = "es测试普通查询接口")
    @RequestMapping(value = "/query/data", method = RequestMethod.GET)
    public ResponseBean testESFind() {
        // 普通查询，默认只查询出10条，需要设置index.max_result_window参数
        SearchRequest searchRequest = new SearchRequest("test_es");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //如果用name直接查询，其实是匹配name分词过后的索引查到的记录(倒排索引)；如果用name.keyword查询则是不分词的查询，正常查询到的记录
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("birthday").from("1991-01-01").to("2010-10-10").format("yyyy-MM-dd");//范围查询
//        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name.keyword", name);//精准查询
        PrefixQueryBuilder prefixQueryBuilder = QueryBuilders.prefixQuery("name.keyword", "张");//前缀查询
//        WildcardQueryBuilder wildcardQueryBuilder = QueryBuilders.wildcardQuery("name.keyword", "*三");//通配符查询
//        FuzzyQueryBuilder fuzzyQueryBuilder = QueryBuilders.fuzzyQuery("name", "三");//模糊查询
        FieldSortBuilder fieldSortBuilder = SortBuilders.fieldSort("age");//按照年龄排序
        fieldSortBuilder.sortMode(SortMode.MIN);//从小到大排序

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(rangeQueryBuilder).should(prefixQueryBuilder);//and or  查询

        sourceBuilder.query(boolQueryBuilder).sort(fieldSortBuilder);//多条件查询
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        searchRequest.source(sourceBuilder);
        return getResponseBean(searchRequest);
    }

    @ApiOperation(value = "es测试聚合查询接口", notes = "es测试聚合查询接口")
    @RequestMapping(value = "/query/data/agg", method = RequestMethod.GET)
    public ResponseBean testESFindAgg() {
        SearchRequest searchRequest = new SearchRequest("test_es");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders.terms("by_age").field("age");
        sourceBuilder.aggregation(termsAggregationBuilder);

        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        searchRequest.source(sourceBuilder);

        try {
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            Aggregations aggregations = searchResponse.getAggregations();
            Map<String, Aggregation> stringAggregationMap = aggregations.asMap();
            ParsedLongTerms parsedLongTerms = (ParsedLongTerms) stringAggregationMap.get("by_age");
            List<? extends Terms.Bucket> buckets = parsedLongTerms.getBuckets();
            Map<Integer, Long> map = new HashMap<>();
            for (Terms.Bucket bucket : buckets) {
                long docCount = bucket.getDocCount();//个数
                Number keyAsNumber = bucket.getKeyAsNumber();//年龄
                System.err.println(keyAsNumber + "岁的有" + docCount + "个");
                map.put(keyAsNumber.intValue(), docCount);
            }
            return new ResponseBean(200, "查询成功", map);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @ApiOperation(value = "es测试深度分页查询接口", notes = "es测试深度分页查询接口")
    @RequestMapping(value = "/query/data/bypage/deep", method = RequestMethod.GET)
    public ResponseBean testQueryDataByPage(@RequestParam("from") Integer from, @RequestParam("size") Integer size) {
        SearchRequest searchRequest = new SearchRequest("qjc_es");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        PrefixQueryBuilder prefixQueryBuilder = QueryBuilders.prefixQuery("address", "北京");//前缀查询
        sourceBuilder.query(prefixQueryBuilder).from(from).size(size);
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        searchRequest.source(sourceBuilder);
        return getResponseBean(searchRequest);
    }

    private ResponseBean getResponseBean(SearchRequest searchRequest) {
        try {
            SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            SearchHits hits = response.getHits();
            JSONArray jsonArray = new JSONArray();
            for (SearchHit hit : hits) {
                String sourceAsString = hit.getSourceAsString();
                JSONObject jsonObject = JSON.parseObject(sourceAsString);
                jsonArray.add(jsonObject);
            }
            return new ResponseBean(200, "查询成功", jsonArray);
        } catch (IOException e) {
            e.printStackTrace();
            return new ResponseBean(10001, "查询失败", null);
        }
    }

    @ApiOperation(value = "es测试快照查询所有数据接口", notes = "es测试快照查询所有数据接口")
    @RequestMapping(value = "/query/alldata/snapshot", method = RequestMethod.GET)
    public ResponseBean testQueryAllData() {
        try {
            SearchRequest searchRequest = new SearchRequest("qjc_es");
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            PrefixQueryBuilder prefixQueryBuilder = QueryBuilders.prefixQuery("address", "北京");//前缀查询
            sourceBuilder.query(prefixQueryBuilder);
            sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
            searchRequest.source(sourceBuilder);
            Scroll scroll = new Scroll(new TimeValue(60, TimeUnit.SECONDS));
            searchRequest.scroll(scroll);
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            String scrollId = searchResponse.getScrollId();
            SearchHit[] hits = searchResponse.getHits().getHits();
            List<SearchHit> resultSearchHit = new ArrayList<>();
            while (ArrayUtils.isNotEmpty(hits)) {
                for (SearchHit hit : hits) {
                    resultSearchHit.add(hit);
                }
                SearchScrollRequest searchScrollRequest = new SearchScrollRequest(scrollId);
                searchScrollRequest.scroll(scroll);
                SearchResponse searchScrollResponse = restHighLevelClient.scroll(searchScrollRequest, RequestOptions.DEFAULT);
                scrollId = searchScrollResponse.getScrollId();
                hits = searchScrollResponse.getHits().getHits();
            }
            //及时清除es快照，释放资源
            ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
            clearScrollRequest.addScrollId(scrollId);
            restHighLevelClient.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
            return new ResponseBean(200, "查询成功", resultSearchHit);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @ApiOperation(value = "es测试更新接口", notes = "es测试更新接口")
    @RequestMapping(value = "/update/data", method = RequestMethod.GET)
    public ResponseBean testESUpdate(@RequestParam String id, @RequestParam Double money) {
        UpdateRequest updateRequest = new UpdateRequest("test_es", id);
        Map<String, Object> map = new HashMap<>();
        map.put("money", money);
        updateRequest.doc(map);
        try {
            UpdateResponse updateResponse = restHighLevelClient.update(updateRequest, RequestOptions.DEFAULT);
            if (updateResponse.getResult() == DocWriteResponse.Result.UPDATED) {
                return new ResponseBean(200, "更新成功", null);
            } else {
                return new ResponseBean(10002, "删除失败", null);
            }
        } catch (IOException e) {
            e.printStackTrace();
            return new ResponseBean(1003, "删除异常", null);
        }
    }

    @ApiOperation(value = "es测试删除接口", notes = "es测试删除接口")
    @RequestMapping(value = "/delete/data", method = RequestMethod.GET)
    public ResponseBean testESDelete(@RequestParam String id, @RequestParam String indexName) {
        DeleteRequest deleteRequest = new DeleteRequest(indexName);
        deleteRequest.id(id);
        try {
            DeleteResponse deleteResponse = restHighLevelClient.delete(deleteRequest, RequestOptions.DEFAULT);
            if (deleteResponse.getResult() == DocWriteResponse.Result.NOT_FOUND) {
                return new ResponseBean(1001, "删除失败", null);
            } else {
                return new ResponseBean(200, "删除成功", null);
            }
        } catch (IOException e) {
            e.printStackTrace();
            return new ResponseBean(1003, "删除异常", null);
        }
    }
}
