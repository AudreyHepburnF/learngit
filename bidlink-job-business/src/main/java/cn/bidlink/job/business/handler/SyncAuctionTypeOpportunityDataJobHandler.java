package cn.bidlink.job.business.handler;

import cn.bidlink.job.common.constant.BusinessConstant;
import cn.bidlink.job.common.utils.ElasticClientUtil;
import cn.bidlink.job.common.utils.SyncTimeUtil;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.annotation.JobHander;
import org.elasticsearch.index.query.QueryBuilders;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * @author <a href="mailto:zhihuizhou@ebnew.com">wisdom</a>
 * @version Ver 1.0
 * @description:同步竞价项目商机
 * @Date 2018/9/17
 */
@JobHander(value = "syncAuctionTypeOpportunityDataJobHandler")
@Service
public class SyncAuctionTypeOpportunityDataJobHandler extends AbstractSyncOpportunityDataJobHandler implements InitializingBean {

    private String PROVINCE = "province";

    @Override
    public ReturnT<String> execute(String... strings) throws Exception {
        SyncTimeUtil.setCurrentDate();
        logger.info("同步协同竞价项目的商机开始");
        syncOpportunityData();
        logger.info("同步协同竞价项目的商机结束");
        return ReturnT.SUCCESS;
    }

    private void syncOpportunityData() {
        Timestamp lastSyncTime = ElasticClientUtil.getMaxTimestamp(elasticClient,
                "cluster.index",
                "cluster.type.supplier_opportunity",
                QueryBuilders.boolQuery()
                        .must(QueryBuilders.termQuery(PROJECT_TYPE, AUCTION_PROJECT_TYPE))
                        .must(QueryBuilders.termQuery(BusinessConstant.PLATFORM_SOURCE_KEY, BusinessConstant.IXIETONG_SOURCE)));
        logger.info("协同竞价项目商机同步时间lastSyncTime：" + SyncTimeUtil.toDateString(lastSyncTime) + "\n," +
                SyncTimeUtil.currentDateToString());
        syncAuctionProjectDataService(lastSyncTime);
        // 修复商机状态
//        fixExpiredAuctionTypeOpportunityData();
    }

//    /**
//     * 修复商机状态
//     */
//    private void fixExpiredAuctionTypeOpportunityData() {
//        logger.info("开始修复商机截止时间状态");
//        Properties properties = elasticClient.getProperties();
//        String currentTime = SyncTimeUtil.toDateString(SyncTimeUtil.getCurrentDate());
//        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.termQuery(PROJECT_TYPE, PURCHASE_PROJECT_TYPE))
//                .must(QueryBuilders.termQuery(BusinessConstant.PLATFORM_SOURCE_KEY, BusinessConstant.IXIETONG_SOURCE))
//                .must(QueryBuilders.rangeQuery(SyncTimeUtil.SYNC_TIME).lte(currentTime));
//        int batchSize = 1000;
//        SearchResponse scrollResp = elasticClient.getTransportClient().prepareSearch(properties.getProperty("cluster.index"))
//                .setTypes(properties.getProperty("cluster.type.supplier_opportunity"))
//                .setSize(batchSize)
//                .setScroll(new TimeValue(60000))
//                .setQuery(boolQueryBuilder).execute().actionGet();
//
//        do {
//            SearchHits hits = scrollResp.getHits();
//            List<Long> projectIds = new ArrayList<>();
//            for (SearchHit hit : hits) {
//                Long projectId = Long.valueOf(hit.getSource().get(PROJECT_ID).toString());
//                projectIds.add(projectId);
//            }
//            doFixExpiredPurchaseTypeOpportunityData(projectIds, SyncTimeUtil.getCurrentDate());
//            scrollResp = elasticClient.getTransportClient().prepareSearchScroll(scrollResp.getScrollId())
//                    .setScroll(new TimeValue(60000))
//                    .execute().actionGet();
//        } while (scrollResp.getHits().getHits().length != 0);
//        logger.info("结束修复商机截止时间状态");
//    }
//
//    private void doFixExpiredPurchaseTypeOpportunityData(List<Long> projectIds, Timestamp lastSyncTime) {
//        if (!CollectionUtils.isEmpty(projectIds)) {
//            String countTemplateSql = "SELECT\n"
//                    + "   count(1)\n"
//                    + "FROM\n"
//                    + "   purchase_project pp\n"
//                    + "WHERE pp.id in (%s) AND pp.quote_stop_time < ?";
//            String queryTemplateSql = "SELECT\n"
//                    + "   s.*, ppi.id AS directoryId,ppi.`name` AS directoryName\n"
//                    + "FROM\n"
//                    + "   (\n"
//                    + "      SELECT\n"
//                    + "         pp.company_id AS purchaseId,\n"
//                    + "         pp.company_name AS purchaseName,\n"
//                    + "         pp.id AS projectId,\n"
//                    + "         pp. code AS projectCode,\n"
//                    + "         pp.`name` AS projectName,\n"
//                    + "         ppc.project_open_range AS openRangeType,\n"
//                    + "         ppc.is_core AS isCore,\n"
//                    + "         pp.project_status AS projectStatus,\n"
//                    + "         pp.process_status AS processStatus,\n"
//                    + "         pp.quote_stop_time AS quoteStopTime,\n"
//                    + "         pp.real_quote_stop_time AS realQuoteStopTime,\n"
//                    + "         pp.zone_str AS areaStr,\n"
//                    + "         pp.province AS province,\n"
//                    + "         pp.link_man AS linkMan,\n"
//                    + "         pp.sys_id AS sourceId,\n"
//                    + "         CASE\n"
//                    + "      WHEN ppc.is_show_mobile = 1 THEN\n"
//                    + "         pp.link_phone\n"
//                    + "      WHEN ppc.is_show_tel = 1 THEN\n"
//                    + "         pp.link_tel\n"
//                    + "      ELSE\n"
//                    + "         NULL\n"
//                    + "      END AS linkPhone,\n"
//                    + "      pp.create_time AS createTime,\n"
//                    + "      pp.update_time AS updateTime\n"
//                    + "   FROM\n"
//                    + "      purchase_project pp\n"
//                    + "   LEFT JOIN purchase_project_control ppc ON pp.id = ppc.id\n"
//                    + "   AND pp.company_id = ppc.company_id\n"
//                    + "   WHERE\n"
//                    + "   pp.process_status > 13\n"
//                    + "   AND ppc.project_open_range = 1 AND pp.id in (%s) AND pp.quote_stop_time < ?\n"
//                    + "   LIMIT ?,?\n"
//                    + "   ) s\n"
//                    + "LEFT JOIN purchase_project_item ppi ON s.projectId = ppi.project_id\n"
//                    + "AND s.purchaseId = ppi.company_id";
//
//            String countSql = String.format(countTemplateSql, StringUtils.collectionToCommaDelimitedString(projectIds));
//            String querySql = String.format(queryTemplateSql, StringUtils.collectionToCommaDelimitedString(projectIds));
//            doSyncProjectDataService(purchaseDataSource, countSql, querySql, Collections.singletonList((Object) lastSyncTime));
//        }
//    }

    /**
     * 同步竞价项目
     *
     * @param lastSyncTime
     */
    private void syncAuctionProjectDataService(Timestamp lastSyncTime) {
        String countUpdatedSql = "SELECT\n" +
                "\tcount(1)\n" +
                "FROM\n" +
                "\tauction_project \n" +
                "WHERE\n" +
                "\tprocess_status > 13 AND update_time > ?";
        String queryUpdatedSql = "SELECT\n" +
                "                    s.*, api.id AS directoryId,api.`name` AS directoryName\n" +
                "                 FROM\n" +
                "                    (\n" +
                "                       SELECT\n" +
                "                          ap.company_id AS purchaseId,\n" +
                "                          ap.company_name AS purchaseName,\n" +
                "                          ap.id AS projectId,\n" +
                "                          ap. code AS projectCode,\n" +
                "                          ap.`name` AS projectName,\n" +
                "                          ap.province,\n" +
//                "                        apc.project_open_range AS openRangeType,\n" +
//                "                        apc.is_core AS isCore,\n" +
                "                          ap.project_status AS projectStatus,\n" +
                "                          ap.process_status AS processStatus,\n" +
                "                          ap.node AS node,\n" +
                "                          if(ap.auction_real_stop_time is NULL,ap.auction_end_time,ap.auction_real_stop_time) AS quoteStopTime,\n" +
                "                          ap.zone_str AS areaStr,\n" +
                "                          ap.sys_id AS sourceId,\n" +
                "                       ap.create_time AS createTime,\n" +
                "                       ap.update_time AS updateTime\n" +
                "                    FROM\n" +
                "                       auction_project ap\n" +
                "                    LEFT JOIN auction_project_control apc ON ap.id = apc.id\n" +
                "                    AND ap.company_id = apc.company_id\n" +
                "                    WHERE\n" +
                "                    ap.process_status > 13\n" +
//                "                       AND apc.project_open_range = 1 \n" +
                "                     AND ap.update_time > ?\n" +
                "                     LIMIT ?,?\n" +
                "                    ) s\n" +
                "                 LEFT JOIN auction_project_item api ON s.projectId = api.project_id\n" +
                "                 AND s.purchaseId = api.company_id;";
        doSyncProjectDataService(auctionDataSource, countUpdatedSql, queryUpdatedSql, Collections.singletonList((Object) lastSyncTime));
    }

    /**
     * 解析商机数据
     *
     * @param currentDate     当前同步的时间
     * @param resultToExecute 需要更新的数据
     * @param result          从数据库获取的数据
     */
    @Override
    protected void parseOpportunity(Timestamp currentDate, List<Map<String, Object>> resultToExecute, Map<String, Object> result) {
        // 项目竞价中
        int AUCTION_NODE = 1;
        int node = (int) result.get(NODE);
        Timestamp quoteStopTime = (Timestamp) result.get(QUOTE_STOP_TIME);
        // 小于截止时间且待截标且竞价中，则为商机，否则不是商机
        if (currentDate.before(quoteStopTime) && node == AUCTION_NODE) {
            result.put(STATUS, VALID_OPPORTUNITY_STATUS);
        } else {
            result.put(STATUS, INVALID_OPPORTUNITY_STATUS);
        }
        resultToExecute.add(appendIdToResult(result));
    }

    @Override
    protected void refresh(Map<String, Object> result, Map<Long, Set<DirectoryEntity>> projectDirectoryMap) {
        super.refresh(result, projectDirectoryMap);

        result.put(QUOTE_STOP_TIME, SyncTimeUtil.toDateString(result.get(QUOTE_STOP_TIME)));
        // 移除不需要的属性
        result.remove(NODE);
        result.remove(PROVINCE);
        // 添加不分词的areaStr
        result.put(AREA_STR_NOT_ANALYZED, result.get(AREA_STR));
        // 项目类型
        result.put(PROJECT_TYPE, AUCTION_PROJECT_TYPE);

        //添加平台来源
        result.put(BusinessConstant.PLATFORM_SOURCE_KEY, BusinessConstant.IXIETONG_SOURCE);
    }


    @Override
    public void afterPropertiesSet() throws Exception {
        execute();
    }
}
