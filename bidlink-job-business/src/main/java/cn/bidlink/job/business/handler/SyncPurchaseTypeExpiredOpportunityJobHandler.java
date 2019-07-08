package cn.bidlink.job.business.handler;

import cn.bidlink.job.common.constant.BusinessConstant;
import cn.bidlink.job.common.es.ElasticClient;
import cn.bidlink.job.common.utils.SyncTimeUtil;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.handler.annotation.JobHander;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@JobHander(value = "syncPurchaseTypeExpiredOpportunityJobHandler")
@Service
public class SyncPurchaseTypeExpiredOpportunityJobHandler extends IJobHandler /*implements InitializingBean*/ {

    private Logger logger= LoggerFactory.getLogger(SyncPurchaseTypeExpiredOpportunityJobHandler.class);

    @Autowired
    private ElasticClient elasticClient;

    // 有效的商机
    private int VALID_OPPORTUNITY_STATUS   = 1;
    // 无效的商机
    private int INVALID_OPPORTUNITY_STATUS = -1;
    // 采购项目类型
    protected int PURCHASE_PROJECT_TYPE      = 2;

    private String BID_STOP_TYPE      = "bidStopType";
    // 自动截标
    private int AUTO_STOP_TYPE   = 2;


    protected String PROJECT_TYPE                = "projectType";
    private String STATUS                      = "status";
    private String QUOTE_STOP_TIME             = "quoteStopTime";

    @Override
    public ReturnT<String> execute(String... strings) throws Exception {
        SyncTimeUtil.setCurrentDate();
        logger.info("修复采购项目的商机开始");
        fixExpiredPurchaseTypeOpportunityData();
        logger.info("修复采购项目的商机结束");
        return ReturnT.SUCCESS;
    }

    /**
     * 修复商机状态
     */
    private void fixExpiredPurchaseTypeOpportunityData() {
        logger.info("开始修复采购项目商机截止时间状态");
        Properties properties = elasticClient.getProperties();
        String currentTime = SyncTimeUtil.toDateString(SyncTimeUtil.getCurrentDate());
        //隆道云的采购项目只有自动截标（即时间到了不能报价） 悦采的采购项目分为手动截标和自动截标
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        boolQuery.should(QueryBuilders.termQuery(BusinessConstant.PLATFORM_SOURCE_KEY, BusinessConstant.IXIETONG_SOURCE))
                .should(QueryBuilders.boolQuery().must(QueryBuilders.termQuery(BusinessConstant.PLATFORM_SOURCE_KEY, BusinessConstant.YUECAI_SOURCE))
                        .must((QueryBuilders.termQuery(BID_STOP_TYPE,AUTO_STOP_TYPE))));
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.termQuery(PROJECT_TYPE, PURCHASE_PROJECT_TYPE))
                .must(boolQuery)
                .must(QueryBuilders.termQuery(STATUS,VALID_OPPORTUNITY_STATUS))
                .must(QueryBuilders.rangeQuery(QUOTE_STOP_TIME).lte(currentTime));
        int batchSize = 1000;
        SearchResponse scrollResp = elasticClient.getTransportClient().prepareSearch(properties.getProperty("cluster.opportunity_index"))
                .setTypes(properties.getProperty("cluster.type.supplier_opportunity"))
                .setSize(batchSize)
                .setScroll(new TimeValue(60000))
                .setFetchSource(false)
                .setQuery(boolQueryBuilder).execute().actionGet();

        do {
            SearchHits hits = scrollResp.getHits();
            BulkRequestBuilder bulkRequest = elasticClient.getTransportClient().prepareBulk();
            Map<String,Object> hashMap = new HashMap<>(1);
            hashMap.put(STATUS,INVALID_OPPORTUNITY_STATUS);
            for (SearchHit hit : hits) {
                bulkRequest.add(elasticClient.getTransportClient()
                        .prepareUpdate(elasticClient.getProperties().getProperty("cluster.opportunity_index"),
                                elasticClient.getProperties().getProperty("cluster.type.supplier_opportunity"),
                               hit.getId())
                        .setDoc(hashMap));
            }
            logger.info("修复采购项目商机的条数为{}",hits.totalHits);
            if(hits.totalHits>0){
                BulkResponse response = bulkRequest.execute().actionGet();
                if (response.hasFailures()) {
                    logger.error(response.buildFailureMessage());
                }
            }
            scrollResp = elasticClient.getTransportClient().prepareSearchScroll(scrollResp.getScrollId())
                    .setScroll(new TimeValue(60000))
                    .execute().actionGet();
        } while (scrollResp.getHits().getHits().length != 0);
        logger.info("结束修复采购项目商机截止时间状态");
    }

    /*@Override
    public void afterPropertiesSet() throws Exception {
        execute();
    }*/
}
