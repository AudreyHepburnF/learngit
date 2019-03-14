package cn.bidlink.job.common.utils;

import cn.bidlink.job.common.es.ElasticClient;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.max.Max;

import java.sql.Timestamp;
import java.util.Properties;

/**
 * @author : <a href="mailto:zikaifeng@ebnew.com">冯子恺</a>
 * @version : Ver 1.0
 * @description :
 * @date : 2017/8/17
 */
public class ElasticClientUtil {
    public static Timestamp getMaxTimestamp(ElasticClient elasticClient, String index, String type, QueryBuilder queryBuilder) {
        Properties properties = elasticClient.getProperties();
        String MAX_SYNC_TIME = "max_syncTime";
        SearchResponse response = elasticClient.getTransportClient().prepareSearch(properties.getProperty(index))
                .setTypes(properties.getProperty(type))
                .setSize(0)
                .setQuery(queryBuilder == null ? QueryBuilders.boolQuery() : queryBuilder)
                .addAggregation(AggregationBuilders.max(MAX_SYNC_TIME).field(SyncTimeUtil.SYNC_TIME))
                .execute()
                .actionGet();
        Max maxSyncTime = response.getAggregations().get(MAX_SYNC_TIME);
        double syncTime = maxSyncTime.getValue();
        if (Double.isInfinite(syncTime) || Double.isNaN(syncTime)) {
            return new Timestamp(0);
        } else {
            // es默认的时区是UTC  多减一个小时,防止拉去数据时间临界值问题
            return new Timestamp((long) syncTime - 8 * 3600 * 1000 - 1 * 3600 * 1000);
        }
    }
}
