package cn.bidlink.job.business.handler;

import cn.bidlink.job.common.es.ElasticClient;
import cn.bidlink.job.common.utils.DBUtil;
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
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.DigestUtils;
import org.springframework.util.StringUtils;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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

    @Autowired
    @Qualifier("proDataSource")
    private DataSource proDataSource;


    @Value("${pageSize:200}")
    protected int pageSize;

    private String DIRECTORY_NAME         = "directoryNameAlias";
    private String SUPPLIER_ID            = "supplierId";

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
        clearExpiredProData();
    }

    /**
     * 清理过期的标王关键字
     */
    private void clearExpiredProData() {
        logger.info("清理失效的标王关键字开始");
        Properties properties = elasticClient.getProperties();
        String countInsertedSql = "SELECT count(1) FROM user_wfirst_use WHERE ENABLE_DISABLE = 2 OR STATE = 2";
        String queryInsertedSql = "SELECT\n"
                                  + "   ufu.COMPANY_ID AS supplierId,\n"
                                  + "   w.KEY_WORD AS directoryNameAlias,\n"
                                  + "   3 AS supplierDirectoryRel,\n"
                                  + "   ufu.CREATE_TIME AS createTime,\n"
                                  + "   ufu.UPDATE_TIME AS updateTime\n"
                                  + "FROM\n"
                                  + "   user_wfirst_use ufu\n"
                                  + "LEFT JOIN wfirst w ON ufu.WFIRST_ID = w.ID\n"
                                  + "WHERE ufu.ENABLE_DISABLE = 2 OR ufu.STATE = 2\n"
                                  + "LIMIT ?, ?";
        long count = DBUtil.count(proDataSource, countInsertedSql, null);
        logger.debug("执行countSql : {}, params : {}，共{}条", countInsertedSql, null, count);
        if (count > 0) {
            for (long i = 0; i < count; ) {
                List<Object> paramsToUse = new ArrayList<>();
                paramsToUse.add(i);
                paramsToUse.add(pageSize);
                List<Map<String, Object>> results = DBUtil.query(proDataSource, queryInsertedSql, paramsToUse);
                logger.debug("执行querySql : {}, params : {}，共{}条", queryInsertedSql, paramsToUse, results.size());
                List<String> ids = new ArrayList<>();
                for (Map<String, Object> result : results) {
                    ids.add(generateSupplierProductId(result));
                }

                DeleteByQueryResponse deleteByQueryResponse = new DeleteByQueryRequestBuilder(elasticClient.getTransportClient(), DeleteByQueryAction.INSTANCE)
                        .setIndices(properties.getProperty("cluster.index"))
                        .setTypes(properties.getProperty("cluster.type.supplier_product"))
                        .setQuery(QueryBuilders.termsQuery("id", ids))
                        .execute()
                        .actionGet();

                if (deleteByQueryResponse.getTotalFailed() > 0) {
                    logger.error("清理失效的标王关键字失败！");
                }
                i += pageSize;
            }
        }
        logger.info("清理失效的标王关键字结束");
    }

    private String generateSupplierProductId(Map<String, Object> result) {
        Long supplierId = (Long) result.get(SUPPLIER_ID);
        String directoryName = String.valueOf(result.get(DIRECTORY_NAME));
        if (supplierId == null) {
            throw new RuntimeException("供应商产品ID生成失败，原因：供应商ID为空!");
        }
        if (StringUtils.isEmpty(directoryName)) {
            throw new RuntimeException("供应商产品ID生成失败，原因：directoryName为null!");
        }

        return DigestUtils.md5DigestAsHex((supplierId + "_" + directoryName).getBytes());
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
