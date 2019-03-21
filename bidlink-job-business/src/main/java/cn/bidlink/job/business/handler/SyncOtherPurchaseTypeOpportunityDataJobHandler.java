package cn.bidlink.job.business.handler;

import cn.bidlink.job.common.constant.BusinessConstant;
import cn.bidlink.job.common.utils.ElasticClientUtil;
import cn.bidlink.job.common.utils.SyncTimeUtil;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.annotation.JobHander;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.sql.Timestamp;
import java.util.*;

/**
 * @author <a href="mailto:zhihuizhou@ebnew.com">wisdom</a>
 * @version Ver 1.0
 * @description:第三方平台采购商机数据
 * @Date 2018/9/19
 */
@Service
@JobHander(value = "syncOtherPurchaseTypeOpportunityDataJobHandler")
public class SyncOtherPurchaseTypeOpportunityDataJobHandler extends AbstractSyncOpportunityDataJobHandler /*implements InitializingBean*/ {


    @Override
    public ReturnT<String> execute(String... strings) throws Exception {
        SyncTimeUtil.setCurrentDate();
        logger.info("开始同步第三方平台采购商机数据");
        syncOtherPurchaseOpportunityData();
        logger.info("结束同步第三方平台采购商机数据");
        return ReturnT.SUCCESS;
    }

    private void syncOtherPurchaseOpportunityData() {
        Timestamp lastSyncTime = ElasticClientUtil.getMaxTimestamp(elasticClient, "cluster.opportunity_index", "cluster.type.supplier_opportunity",
                QueryBuilders.boolQuery().must(QueryBuilders.termQuery(BusinessConstant.PLATFORM_SOURCE_KEY, BusinessConstant.OTHER_SOURCE))
                        .must(QueryBuilders.termQuery(PROJECT_TYPE, PURCHASE_PROJECT_TYPE)));
//        Timestamp lastSyncTime = SyncTimeUtil.GMT_TIME;
        logger.info("同步第三方平台采购商机lastSyncTime:" + SyncTimeUtil.toDateString(lastSyncTime) + "\n" + ",syncTime:" + SyncTimeUtil.currentDateToString());
        syncOtherPurchaseOpportunityDataService(lastSyncTime);
        // 修复采购商机时间截止
//        fixExpiredOtherPurchaseTypeOpportunityDataService();
    }

    private void fixExpiredOtherPurchaseTypeOpportunityDataService() {
        logger.info("开始修复采购商机截止时间");
        Properties properties = elasticClient.getProperties();
        int batchSize = 100;
        String currentDate = SyncTimeUtil.currentDateToString();
        SearchResponse scrollResponse = elasticClient.getTransportClient().prepareSearch(properties.getProperty("cluster.opportunity_index"))
                .setTypes(properties.getProperty("cluster.type.supplier_opportunity"))
                .setQuery(QueryBuilders.boolQuery()
                        .must(QueryBuilders.termQuery(BusinessConstant.PLATFORM_SOURCE_KEY, BusinessConstant.OTHER_SOURCE))
                        .must(QueryBuilders.termQuery(PROJECT_TYPE, PURCHASE_PROJECT_TYPE))
                        .must(QueryBuilders.rangeQuery(SyncTimeUtil.SYNC_TIME).lte(currentDate)))
                .setScroll(new TimeValue(60000))
                .setFetchSource(new String[]{PROJECT_ID}, null)
                .setSize(batchSize)
                .execute().actionGet();

        do {
            SearchHit[] hits = scrollResponse.getHits().getHits();
            List<Long> projectIds = new ArrayList<>();
            for (SearchHit hit : hits) {
                projectIds.add(Long.valueOf(hit.getSourceAsMap().get(PROJECT_ID).toString()));
            }
            doFixExpiredOtherPurchaseTypeOpportunityDataService(projectIds, SyncTimeUtil.getCurrentDate());
            scrollResponse = elasticClient.getTransportClient().prepareSearchScroll(scrollResponse.getScrollId())
                    .setScroll(new TimeValue(60000))
                    .execute().actionGet();
        } while (scrollResponse.getHits().getHits().length != 0);
        logger.info("采购商机修复截止时间完成");
    }

    private void doFixExpiredOtherPurchaseTypeOpportunityDataService(List<Long> projectIds, Timestamp currentDate) {
        if (!CollectionUtils.isEmpty(projectIds)) {
            String countSqlTemplate = "SELECT\n" +
                    "\tcount( 1 ) \n" +
                    "FROM\n" +
                    "\tpurchase_information \n" +
                    "WHERE\n" +
                    "\tis_complete = 1" +
                    "\tAND id in (%s) and quote_stop_time < ?";

            String querySqlTemplate = "SELECT\n" +
                    "                    s.*, pii.id AS directoryId,pii.`name` AS directoryName\n" +
                    "                 FROM\n" +
                    "                    (\n" +
                    "                       SELECT\n" +
                    "                          pi.company_id AS purchaseId,\n" +
                    "                          pi.company_name AS purchaseName,\n" +
                    "                          pi.id AS projectId,\n" +
                    "                          pi. code AS projectCode,\n" +
                    "                          pi.`name` AS projectName,\n" +
                    "                          pi.quote_stop_time AS quoteStopTime,\n" +
                    "                          pi.real_quote_stop_time AS realQuoteStopTime,\n" +
                    "                          pi.zone_str AS areaStr,\n" +
                    "                          pi.province AS province," +
                    "                          pi.create_time AS createTime,\n" +
                    "                          pi.update_time AS updateTime\n" +
                    "                    FROM" +
                    "                       purchase_information pi                 \n" +
                    "                    WHERE" +
                    "                    pi.is_complete = 1" +
                    "                    and pi.id in (%s)" +
                    "                    and quote_stop_time < ?" +
                    "                    LIMIT ?,?\n" +
                    "                    ) s\n" +
                    "                 LEFT JOIN purchase_information_item pii ON s.projectId = pii.project_id\n" +
                    "                 AND s.purchaseId = pii.company_id";
            String countSql = String.format(countSqlTemplate, StringUtils.collectionToCommaDelimitedString(projectIds));
            String querySql = String.format(querySqlTemplate, StringUtils.collectionToCommaDelimitedString(projectIds));
            doSyncProjectDataService(apiDataSource, countSql, querySql, Collections.singletonList(currentDate), UPDATE_OPERATION);
        }
    }

    private void syncOtherPurchaseOpportunityDataService(Timestamp lastSyncTime) {
        syncOtherPurchaseOpportunityInsertDataService(lastSyncTime);
        syncOtherPurchaseOpportunityUpdateDataService(lastSyncTime);
    }

    private void syncOtherPurchaseOpportunityInsertDataService(Timestamp lastSyncTime) {
        String countSql = "SELECT\n" +
                "\tcount( 1 ) \n" +
                "FROM\n" +
                "\tpurchase_information \n" +
                "WHERE\n" +
                "\tis_complete = 1" +
                "\tAND create_time > ?";

        String querySql = "SELECT\n" +
                "                    s.*, pii.id AS directoryId,pii.`name` AS directoryName\n" +
                "                 FROM\n" +
                "                    (\n" +
                "                       SELECT\n" +
                "                          pi.company_id AS purchaseId,\n" +
                "                          pi.company_name AS purchaseName,\n" +
                "                          pi.id AS projectId,\n" +
                "                          pi. code AS projectCode,\n" +
                "                          pi.`name` AS projectName,\n" +
                "                          pi.quote_stop_time AS quoteStopTime,\n" +
                "                          pi.real_quote_stop_time AS realQuoteStopTime,\n" +
                "                          pi.zone_str AS areaStr,\n" +
                "                          pi.province AS province," +
                "                          pi.create_time AS createTime,\n" +
                "                          pi.update_time AS updateTime\n" +
                "                    FROM" +
                "                       purchase_information pi                 \n" +
                "                    WHERE" +
                "                    pi.is_complete = 1" +
                "                    and pi.create_time > ?" +
                "                    LIMIT ?,?\n" +
                "                    ) s\n" +
                "                 LEFT JOIN purchase_information_item pii ON s.projectId = pii.project_id\n" +
                "                 AND s.purchaseId = pii.company_id";
        doSyncProjectDataService(apiDataSource, countSql, querySql, Collections.singletonList(lastSyncTime), INSERT_OPERATION);
    }

    private void syncOtherPurchaseOpportunityUpdateDataService(Timestamp lastSyncTime) {
        String countSql = "SELECT\n" +
                "\tcount( 1 ) \n" +
                "FROM\n" +
                "\tpurchase_information \n" +
                "WHERE\n" +
                "\tupdate_time > ?";

        String querySql = "SELECT\n" +
                "                    s.*, pii.id AS directoryId,pii.`name` AS directoryName\n" +
                "                 FROM\n" +
                "                    (\n" +
                "                       SELECT\n" +
                "                          pi.company_id AS purchaseId,\n" +
                "                          pi.company_name AS purchaseName,\n" +
                "                          pi.id AS projectId,\n" +
                "                          pi. code AS projectCode,\n" +
                "                          pi.`name` AS projectName,\n" +
                "                          pi.quote_stop_time AS quoteStopTime,\n" +
                "                          pi.real_quote_stop_time AS realQuoteStopTime,\n" +
                "                          pi.zone_str AS areaStr,\n" +
                "                          pi.province AS province," +
                "                          pi.create_time AS createTime,\n" +
                "                          pi.update_time AS updateTime\n" +
                "                    FROM" +
                "                       purchase_information pi                 \n" +
                "                    WHERE" +
                "                    pi.is_complete = 1" +
                "                    AND pi.update_time > ?" +
                "                    LIMIT ?,?\n" +
                "                    ) s\n" +
                "                 LEFT JOIN purchase_information_item pii ON s.projectId = pii.project_id\n" +
                "                 AND s.purchaseId = pii.company_id";
        doSyncProjectDataService(apiDataSource, countSql, querySql, Collections.singletonList(lastSyncTime), INSERT_OPERATION);
    }

    @Override
    protected void parseOpportunity(Timestamp currentDate, List<Map<String, Object>> resultToExecute, Map<String, Object> result) {
        Timestamp quoteStopTime = ((Timestamp) result.get(QUOTE_STOP_TIME));
        Timestamp realQuoteStopTime = (Timestamp) result.get(REAL_QUOTE_STOP_TIME);
        if (realQuoteStopTime == null && quoteStopTime != null && currentDate.before(quoteStopTime)) {
            result.put(STATUS, VALID_OPPORTUNITY_STATUS);
        } else {
            result.put(STATUS, INVALID_OPPORTUNITY_STATUS);
        }
        resultToExecute.add(appendIdToResult(result, BusinessConstant.OTHER_SOURCE));
    }

    @Override
    protected void refresh(Map<String, Object> result, Map<Long, Set<DirectoryEntity>> projectDirectoryMap) {
        super.refresh(result, projectDirectoryMap);
        // 移除属性
        result.remove(REAL_QUOTE_STOP_TIME);
        result.put(PROJECT_TYPE, PURCHASE_PROJECT_TYPE);
        result.remove(PROVINCE);
        result.put(BusinessConstant.PLATFORM_SOURCE_KEY, BusinessConstant.OTHER_SOURCE);
    }

//    @Override
//    public void afterPropertiesSet() throws Exception {
//        execute();
//    }
}
