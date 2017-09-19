package cn.bidlink.job.business.handler;

import cn.bidlink.job.common.es.ElasticClient;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.handler.annotation.JobHander;
import org.elasticsearch.action.deletebyquery.DeleteByQueryAction;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequestBuilder;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * 删除过期的商机数据
 *
 * @author : <a href="mailto:zikaifeng@ebnew.com">冯子恺</a>
 * @version : Ver 1.0
 * @description :
 * @date : 2017/8/9
 */
@JobHander(value = "clearOpportunityDataJobHandler")
@Service
public class ClearOpportunityDataJobHandler extends IJobHandler /*implements InitializingBean */ {
    private Logger logger = LoggerFactory.getLogger(SyncSupplierProductDataJobHandler.class);

    @Autowired
    private ElasticClient elasticClient;

    @Override
    public ReturnT<String> execute(String... strings) throws Exception {
        logger.info("清理商机数据开始");
        clearOpportunityData();
        logger.info("清理商机数据结束");
        return ReturnT.SUCCESS;
    }

    private void clearOpportunityData() {
        clearSupplierVisitedOpportunityData();
        clearExpiredOpportunityData();
    }

    /**
     * 清理供应商访问的历史记录
     */
    private void clearSupplierVisitedOpportunityData() {
        logger.info("清理供应商访问的历史记录开始");
        Properties properties = elasticClient.getProperties();
        List<String> ids = getExpiredOpportunityIds(properties);
        DeleteByQueryResponse deleteByQueryResponse = new DeleteByQueryRequestBuilder(elasticClient.getTransportClient(), DeleteByQueryAction.INSTANCE)
                .setIndices(properties.getProperty("cluster.index"))
                .setTypes(properties.getProperty("cluster.type.supplier_visited_opportunity"))
                .setQuery(QueryBuilders.termsQuery("opportunityId", ids))
                .execute()
                .actionGet();

        if (deleteByQueryResponse.getTotalFailed() > 0) {
            logger.error("清理供应商访问的历史记录失败！");
        }
        logger.info("清理供应商访问的历史记录结束");
    }

    private List<String> getExpiredOpportunityIds(Properties properties) {
        SearchResponse response = elasticClient.getTransportClient().prepareSearch(properties.getProperty("cluster.index"))
                .setTypes(properties.getProperty("cluster.type.supplier_opportunity"))
                .setQuery(QueryBuilders.termQuery("status", -1))
                .execute()
                .actionGet();

        long totalHits = response.getHits().getTotalHits();
        List<String> ids = new ArrayList<>();
        if (totalHits > 0) {
            for (SearchHit searchHit : response.getHits().hits()) {
                ids.add(searchHit.getId());
            }
        }
        return ids;
    }

    private void clearExpiredOpportunityData() {
        logger.info("清理过期的商机数据开始");
        DeleteByQueryResponse response = new DeleteByQueryRequestBuilder(elasticClient.getTransportClient(), DeleteByQueryAction.INSTANCE)
                .setIndices(elasticClient.getProperties().getProperty("cluster.index"))
                .setTypes(elasticClient.getProperties().getProperty("cluster.type.supplier_opportunity"))
                .setQuery(QueryBuilders.termQuery("status", -1))
                .execute()
                .actionGet();
        if (response.getTotalFailed() > 0) {
            logger.error("清理过期的商机数据失败！");
        }
        logger.info("清理过期的商机数据结束");
    }

//    @Override
//    public void afterPropertiesSet() throws Exception {
//        execute();
//    }
}
