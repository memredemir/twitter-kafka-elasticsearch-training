package com.twittersample.elasticsearch.consumer;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;

public class ElasticSearchConsumerTransaction {

    /**
     * @param bulkRequest BulkRequest.
     * @param index String elasticsearch index.
     * @param json json String that involves the value that will be sent to elasticsearch.
     * @param id indexRequest id.
     */
    public static void addIndexRequestToBulkRequest(BulkRequest bulkRequest, String index, String json, String id) {
        IndexRequest indexRequest = new IndexRequest(index)
                .id(id)
                .source(json, XContentType.JSON);

        bulkRequest.add(indexRequest);
    }

}
