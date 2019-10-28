package com.es.demo.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.es.demo.vo.ResponseBean;
import com.es.demo.vo.User;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.Date;
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
    private RestHighLevelClient highLevelClient;

    @ApiOperation(value = "es测试插入接口", notes = "es测试插入接口")
    @RequestMapping(value = "/insert", method = RequestMethod.POST)
    public ResponseBean findIndustryClassList(@RequestBody User user) {
        String indexName = "test_es";
        IndexRequest indexRequest = new IndexRequest(indexName, "user");

        String userJson = JSONObject.toJSONString(user);

        indexRequest.source(userJson, XContentType.JSON);

        try {
            IndexResponse indexResponse = highLevelClient.index(indexRequest, RequestOptions.DEFAULT);
            if (indexResponse != null) {
                String id = indexResponse.getId();
                String index = indexResponse.getIndex();
                String type = indexResponse.getType();
                long version = indexResponse.getVersion();
                log.info("index:{},type:{},id:{}", index, type, id);
                if (indexResponse.getResult() == DocWriteResponse.Result.CREATED) {
                    System.out.println("新增文档成功!" + index + "-" + type + "-" + id + "-" + version);
                    return new ResponseBean(200, "插入成功", null);
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


    @ApiOperation(value = "es测试查询接口", notes = "es测试查询接口")
    @RequestMapping(value = "/query", method = RequestMethod.GET)
    public ResponseBean testESFind(@RequestParam String name) {
        SearchRequest searchRequest = new SearchRequest("test_es");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("birthday").from("1997-01-01").to("2000-10-10").format("yyyy-MM-dd");
        sourceBuilder.query(rangeQueryBuilder);
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        searchRequest.source(sourceBuilder);
        try {
            SearchResponse response = highLevelClient.search(searchRequest, RequestOptions.DEFAULT);
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

    @ApiOperation(value = "es测试删除接口", notes = "es测试删除接口")
    @RequestMapping(value = "/delete", method = RequestMethod.GET)
    public ResponseBean testESDelete(@RequestParam String id) {
        DeleteRequest deleteRequest = new DeleteRequest("test_es");
        deleteRequest.type("user");
        deleteRequest.id(id);
        try {
            DeleteResponse deleteResponse = highLevelClient.delete(deleteRequest, RequestOptions.DEFAULT);
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
