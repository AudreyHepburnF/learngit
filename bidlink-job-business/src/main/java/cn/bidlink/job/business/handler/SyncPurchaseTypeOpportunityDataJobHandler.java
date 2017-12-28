package cn.bidlink.job.business.handler;

import cn.bidlink.job.common.utils.ElasticClientUtil;
import cn.bidlink.job.common.utils.SyncTimeUtil;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.annotation.JobHander;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.joda.time.DateTime;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.sql.Timestamp;
import java.util.*;


/**
 * 同步商机数据
 *
 * @author : <a href="mailto:zikaifeng@ebnew.com">冯子恺</a>
 * @version : Ver 1.0
 * @description :
 * @date : 2017/8/7
 */
@JobHander(value = "syncPurchaseTypeOpportunityDataJobHandler")
@Service
public class SyncPurchaseTypeOpportunityDataJobHandler extends AbstractSyncOpportunityDataJobHandler /*implements InitializingBean*/ {
    // 自动截标
    private int AUTO_STOP_TYPE   = 2;
    // 手动截标
    private int MANUAL_STOP_TYPE = 1;

    private String PROJECT_STATUS     = "projectStatus";
    private String BID_STOP_TYPE      = "bidStopType";
    private String BID_STOP_TIME      = "bidStopTime";
    private String BID_TRUE_STOP_TIME = "bidTrueStopTime";

    public ReturnT<String> execute(String... strings) throws Exception {
        SyncTimeUtil.setCurrentDate();
        logger.info("同步采购项目的商机开始");
        syncOpportunityData();
        logger.info("同步采购项目的商机结束");
        return ReturnT.SUCCESS;
    }

    /**
     * 同步商机数据，分为采购商机和招标商机
     */
    private void syncOpportunityData() {
        Timestamp lastSyncTime = ElasticClientUtil.getMaxTimestamp(elasticClient,
                                                                   "cluster.index",
                                                                   "cluster.type.supplier_opportunity",
                                                                   QueryBuilders.boolQuery().must(QueryBuilders.termQuery("projectType", PURCHASE_PROJECT_TYPE)));
        logger.info("采购项目商机同步时间：" + new DateTime(lastSyncTime).toString("yyyy-MM-dd HH:mm:ss"));
        syncPurchaseProjectDataService(lastSyncTime);
        fixExpiredAutoStopTypePurchaseProjectDataService(lastSyncTime);
    }

    /**
     * 修复自动截标的数据无法同步的问题
     *
     * @param lastSyncTime
     */
    private void fixExpiredAutoStopTypePurchaseProjectDataService(Timestamp lastSyncTime) {
        logger.info("修复自动截标商机开始");
        Properties properties = elasticClient.getProperties();
        String currentTime = new DateTime(SyncTimeUtil.getCurrentDate()).toString(SyncTimeUtil.DATE_TIME_PATTERN);
        // 查询小于当前时间的自动截标
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("status", 1))
                .must(QueryBuilders.termQuery("projectType", PURCHASE_PROJECT_TYPE))
                .must(QueryBuilders.rangeQuery("syncTime").lt(currentTime));

        int batchSize = 100;
        SearchResponse scrollResp = elasticClient.getTransportClient().prepareSearch(properties.getProperty("cluster.index"))
                .setTypes(properties.getProperty("cluster.type.supplier_opportunity"))
                .setQuery(queryBuilder)
                .setScroll(new TimeValue(60000))
                .setSize(batchSize)
                .get();
        int i = 0;
        do {
            SearchHit[] searchHits = scrollResp.getHits().hits();
            List<Long> projectIds = new ArrayList<>();
            for (SearchHit searchHit : searchHits) {
                projectIds.add(Long.valueOf(String.valueOf(searchHit.getSource().get(PROJECT_ID))));
            }
            doFixExpiredAutoStopTypePurchaseProjectDataService(projectIds, lastSyncTime);
            scrollResp = elasticClient.getTransportClient().prepareSearchScroll(scrollResp.getScrollId())
                    .setScroll(new TimeValue(60000))
                    .execute().actionGet();
        } while (scrollResp.getHits().getHits().length != 0);
        logger.info("修复自动截标商机结束");
    }

    private void doFixExpiredAutoStopTypePurchaseProjectDataService(List<Long> projectIds, Timestamp lastSyncTime) {
        if (!CollectionUtils.isEmpty(projectIds)) {
            String countTemplateSql = "SELECT\n"
                                      + "   count(1)\n"
                                      + "FROM\n"
                                      + "   bmpfjz_project bp\n"
                                      + "JOIN bmpfjz_project_ext bpe ON bp.id = bpe.id\n"
                                      + "WHERE\n"
                                      + "   bpe.bid_stop_type = 2 AND bpe.bid_stop_time < ? AND bpe.id IN (%s)";
            String queryTemplateSql = "SELECT\n"
                                      + "   b.*, bpi.`name` AS directoryName\n"
                                      + "FROM\n"
                                      + "   (\n"
                                      + "      SELECT\n"
                                      + "         bp.comp_id AS purchaseId,\n"
                                      + "         bp.comp_name AS purchaseName,\n"
                                      + "         bp.id AS projectId,\n"
                                      + "         bp.`code` AS projectCode,\n"
                                      + "         bp.`name` AS projectName,\n"
                                      + "         bpe.purchase_open_range_type AS openRangeType,\n"
                                      + "         bp.project_status AS projectStatus,\n"
                                      + "         bpe.bid_stop_type AS bidStopType,\n"
                                      + "         bpe.bid_stop_time AS bidStopTime,\n"
                                      + "         bpe.bid_true_stop_time AS bidTrueStopTime\n"
                                      + "      FROM\n"
                                      + "         bmpfjz_project bp\n"
                                      + "      JOIN bmpfjz_project_ext bpe ON bp.id = bpe.id\n"
                                      + "      WHERE\n"
                                      + "         bpe.bid_stop_type = 2\n"
                                      + "      AND bpe.bid_stop_time < ?\n"
                                      + "      AND bpe.id IN (%s)\n"
                                      + "      LIMIT ?,?\n"
                                      + "   ) b\n"
                                      + "JOIN bmpfjz_project_item bpi ON b.projectId = bpi.project_id order by bpi.create_time";

            String countSql = String.format(countTemplateSql, StringUtils.collectionToCommaDelimitedString(projectIds));
            String querySql = String.format(queryTemplateSql, StringUtils.collectionToCommaDelimitedString(projectIds));
            doSyncProjectDataService(countSql, querySql, Collections.singletonList((Object) lastSyncTime));
        }
    }

    /**
     * 同步采购项目
     *
     * @param lastSyncTime
     */
    private void syncPurchaseProjectDataService(Timestamp lastSyncTime) {
        String countUpdatedSql = "SELECT count(1) FROM\n"
                                 + "   bmpfjz_project bp\n"
                                 + "JOIN bmpfjz_project_ext bpe ON bp.id = bpe.id\n"
                                 + "WHERE\n"
                                 + "   bpe.bid_result_show_type = 1\n"
                                 + "AND bp.update_time > ?\n"
                                 + "AND bp.project_status IN (5, 6, 10)";
        String queryUpdatedSql = "SELECT b.*, bpi.`name` AS directoryName FROM (SELECT\n"
                                 + "   bp.comp_id AS purchaseId,\n"
                                 + "   bp.comp_name AS purchaseName,\n"
                                 + "   bp.id AS projectId,\n"
                                 + "   bp.`code` AS projectCode,\n"
                                 + "   bp.`name` AS projectName,\n"
                                 + "   bpe.purchase_open_range_type AS openRangeType,\n"
                                 + "   bp.is_core AS isCore,\n"
                                 + "   bp.project_status AS projectStatus,\n"
                                 + "   bpe.bid_stop_type AS bidStopType,\n"
                                 + "   bpe.bid_stop_time AS bidStopTime,\n"
                                 + "   bpe.bid_true_stop_time AS bidTrueStopTime,\n"
//                                 + "   bpe.area_str AS areaStr,\n"
                                 + "   bpe.link_man AS linkMan,\n"
                                 + "   CASE\n"
                                 + "WHEN bpe.is_show_mobile = 1 THEN\n"
                                 + "   bpe.link_phone\n"
                                 + "WHEN bpe.is_show_tel = 1 THEN\n"
                                 + "   bpe.link_tel\n"
                                 + "ELSE\n"
                                 + "   NULL\n"
                                 + "END AS linkPhone,\n"
                                 + " bp.create_time AS createTime,\n"
                                 + " bp.update_time AS updateTime\n"
                                 + "FROM\n"
                                 + "   bmpfjz_project bp\n"
                                 + "JOIN bmpfjz_project_ext bpe ON bp.id = bpe.id\n"
                                 + "WHERE\n"
                                 + "   bpe.bid_result_show_type = 1\n"
                                 + "AND bp.update_time > ?\n"
                                 + "AND bp.project_status IN (5, 6, 10)\n"
                                 + "LIMIT ?,\n"
                                 + " ?) b JOIN bmpfjz_project_item bpi ON b.projectId = bpi.project_id order by bpi.id";
        doSyncProjectDataService(countUpdatedSql, queryUpdatedSql, Collections.singletonList((Object) lastSyncTime));
    }

    protected void appendTenantKeyAndAreaStrToResult(List<Map<String, Object>> resultToExecute, Set<Long> purchaseIds) {
        if (purchaseIds.size() > 0) {
            Map<Long, Object> tenantKeyMap = queryTenantKey(purchaseIds);
            Map<Long, Object> areaMap = queryArea(purchaseIds);
            for (Map<String, Object> result : resultToExecute) {
                Long purchaseId = Long.valueOf(String.valueOf(result.get(PURCHASE_ID)));
                result.put(TENANT_KEY, tenantKeyMap.get(purchaseId));
                result.put(AREA_STR, areaMap.get(purchaseId));
            }
        }
    }

    /**
     * 解析商机数据
     *
     * @param currentDate     当前同步的时间
     * @param resultToExecute 需要更新的数据
     * @param result          从数据库获取的数据
     */
    protected void parseOpportunity(Timestamp currentDate, List<Map<String, Object>> resultToExecute, Map<String, Object> result) {
        // 判断商机
        int projectStatus = (int) result.get(PROJECT_STATUS);
        int bidStopType = (int) result.get(BID_STOP_TYPE);
        Timestamp bidStopTime = (Timestamp) result.get(BID_STOP_TIME);
        Timestamp bidTrueStopTime = (Timestamp) result.get(BID_TRUE_STOP_TIME);
        if (bidStopType == AUTO_STOP_TYPE) {
            // 判断时间未过期就是商机
            if (bidStopTime != null && bidStopTime.after(currentDate) && projectStatus == 5) {
                result.put(STATUS, VALID_OPPORTUNITY_STATUS);
                resultToExecute.add(appendIdToResult(result));
            } else {
                result.put(STATUS, INVALID_OPPORTUNITY_STATUS);
                resultToExecute.add(appendIdToResult(result));
            }
        } else if (bidStopType == MANUAL_STOP_TYPE) {
            // 未截标就是商机
            if (bidTrueStopTime == null && projectStatus == 5) {
                result.put(STATUS, VALID_OPPORTUNITY_STATUS);
                resultToExecute.add(appendIdToResult(result));
            } else {
                result.put(STATUS, INVALID_OPPORTUNITY_STATUS);
                resultToExecute.add(appendIdToResult(result));
            }
        } else {
            // no-op
        }
    }


    protected void refresh(Map<String, Object> result, Map<Long, Set<String>> projectDirectoryMap) {
        super.refresh(result, projectDirectoryMap);
        // 移除不需要的属性
        result.remove(PROJECT_STATUS);
        result.remove(BID_STOP_TYPE);
        result.put(QUOTE_STOP_TIME, SyncTimeUtil.toDateString(result.get(BID_STOP_TIME)));
        result.remove(BID_STOP_TIME);
        result.remove(BID_TRUE_STOP_TIME);
        // 项目类型
        result.put(PROJECT_TYPE, PURCHASE_PROJECT_TYPE);
    }


//    @Override
//    public void afterPropertiesSet() throws Exception {
//        execute();
//    }
}
