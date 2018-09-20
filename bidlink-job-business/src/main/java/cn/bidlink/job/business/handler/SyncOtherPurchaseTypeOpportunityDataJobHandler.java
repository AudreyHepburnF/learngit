package cn.bidlink.job.business.handler;

import cn.bidlink.job.common.constant.BusinessConstant;
import cn.bidlink.job.common.utils.ElasticClientUtil;
import cn.bidlink.job.common.utils.SyncTimeUtil;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.annotation.JobHander;
import org.elasticsearch.index.query.QueryBuilders;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
        Timestamp lastSyncTime = ElasticClientUtil.getMaxTimestamp(elasticClient, "cluster.index", "cluster.type.supplier_opportunity",
                QueryBuilders.boolQuery().must(QueryBuilders.termQuery(BusinessConstant.PLATFORM_SOURCE_KEY, BusinessConstant.OTHER_SOURCE))
                        .must(QueryBuilders.termQuery(PROJECT_TYPE, PURCHASE_PROJECT_TYPE)));
        logger.info("同步第三方平台采购商机lastSyncTime:" + SyncTimeUtil.toDateString(lastSyncTime) + "\n" + ",syncTime:" + SyncTimeUtil.currentDateToString());
        syncOtherPurchaseOpportunityDataService(lastSyncTime);
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
                "\tcreate_time > ?";

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
                //                         FIXME  缺少provide区编码的字段,需要通过该字段来判断属于什么区域
//                "                        pi.province AS province," +
                "                          pi.create_time AS createTime,\n" +
                "                          pi.update_time AS updateTime\n" +
                "                    FROM" +
                "                       purchase_information pi                 \n" +
                "                    WHERE" +
                "                    pi.create_time > ?" +
                "                    LIMIT ?,?\n" +
                "                    ) s\n" +
                "                 LEFT JOIN purchase_information_item pii ON s.projectId = pii.project_id\n" +
                "                 AND s.purchaseId = pii.company_id";
        doSyncProjectDataService(apiDataSource, countSql, querySql, Collections.singletonList(lastSyncTime));
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
//                "                        pi.province AS province," +
                "                          pi.create_time AS createTime,\n" +
                "                          pi.update_time AS updateTime\n" +
                "                    FROM" +
                "                       purchase_information pi                 \n" +
                "                    WHERE" +
                "                    pi.update_time > ?" +
                "                    LIMIT ?,?\n" +
                "                    ) s\n" +
                "                 LEFT JOIN purchase_information_item pii ON s.projectId = pii.project_id\n" +
                "                 AND s.purchaseId = pii.company_id";
        doSyncProjectDataService(apiDataSource, countSql, querySql, Collections.singletonList(lastSyncTime));
    }

    @Override
    protected void parseOpportunity(Timestamp currentDate, List<Map<String, Object>> resultToExecute, Map<String, Object> result) {
        Timestamp quoteStopTime = ((Timestamp) result.get(QUOTE_STOP_TIME));
        Timestamp realQuoteStopTime = (Timestamp) result.get(REAL_QUOTE_STOP_TIME);
        if (realQuoteStopTime != null && quoteStopTime != null && currentDate.before(quoteStopTime)) {
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
        result.put(BusinessConstant.PLATFORM_SOURCE_KEY, BusinessConstant.OTHER_SOURCE);
    }

//    @Override
//    public void afterPropertiesSet() throws Exception {
//        execute();
//    }
}
