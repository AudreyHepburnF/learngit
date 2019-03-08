package cn.bidlink.job.business.handler;

import cn.bidlink.job.common.constant.BusinessConstant;
import cn.bidlink.job.common.utils.ElasticClientUtil;
import cn.bidlink.job.common.utils.SyncTimeUtil;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.annotation.JobHander;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.sql.Timestamp;
import java.util.*;


/**
 * @author <a href="mailto:zhihuizhou@ebnew.com">wisdom</a>
 * @version Ver 1.0
 * @description:同步拍卖项目商机
 * @Date 2018/9/17
 */
@JobHander(value = "syncSaleTypeOpportunityDataJobHandler")
@Service
public class SyncSaleTypeOpportunityDataJobHandler extends AbstractSyncOpportunityDataJobHandler /*implements InitializingBean*/ {

    private String PROVINCE = "province";

    private int CANAL_PROJECT_STATUS = 6;

    @Override
    public ReturnT<String> execute(String... strings) throws Exception {
        SyncTimeUtil.setCurrentDate();
        logger.info("同步协同拍卖项目的商机开始");
        syncOpportunityData();
        logger.info("同步协同拍卖项目的商机结束");
        return ReturnT.SUCCESS;
    }

    private void syncOpportunityData() {
        Timestamp lastSyncTime = ElasticClientUtil.getMaxTimestamp(elasticClient,
                "cluster.index",
                "cluster.type.supplier_opportunity",
                QueryBuilders.boolQuery()
                        .must(QueryBuilders.termQuery(PROJECT_TYPE, SALE_PROJECT_TYPE))
                        .must(QueryBuilders.termQuery(BusinessConstant.PLATFORM_SOURCE_KEY, BusinessConstant.IXIETONG_SOURCE)));
        logger.info("协同拍卖项目商机同步时间lastSyncTime：" + SyncTimeUtil.toDateString(lastSyncTime) + "\n," +
                SyncTimeUtil.currentDateToString());
        syncSaleProjectDataService(lastSyncTime);
//         修复商机状态
//        fixExpiredSaleTypeOpportunityData();
    }

    /**
     * 修复商机状态
     */
    private void fixExpiredSaleTypeOpportunityData() {
        logger.info("开始修复商机截止时间状态");
        Properties properties = elasticClient.getProperties();
        String currentTime = SyncTimeUtil.toDateString(SyncTimeUtil.getCurrentDate());
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.termQuery(PROJECT_TYPE, SALE_PROJECT_TYPE))
                .must(QueryBuilders.termQuery(BusinessConstant.PLATFORM_SOURCE_KEY, BusinessConstant.IXIETONG_SOURCE))
                .must(QueryBuilders.rangeQuery(SyncTimeUtil.SYNC_TIME).lte(currentTime));
        int batchSize = 100;
        SearchResponse scrollResp = elasticClient.getTransportClient().prepareSearch(properties.getProperty("cluster.index"))
                .setTypes(properties.getProperty("cluster.type.supplier_opportunity"))
                .setSize(batchSize)
                .setScroll(new TimeValue(60000))
                .setQuery(boolQueryBuilder).execute().actionGet();

        do {
            SearchHits hits = scrollResp.getHits();
            List<Long> projectIds = new ArrayList<>();
            for (SearchHit hit : hits) {
                Long projectId = Long.valueOf(hit.getSource().get(PROJECT_ID).toString());
                projectIds.add(projectId);
            }
            this.doFixExpiredSaleTypeOpportunityData(projectIds, SyncTimeUtil.getCurrentDate());
            scrollResp = elasticClient.getTransportClient().prepareSearchScroll(scrollResp.getScrollId())
                    .setScroll(new TimeValue(60000))
                    .execute().actionGet();
        } while (scrollResp.getHits().getHits().length != 0);
        logger.info("结束修复商机截止时间状态");
    }

    private void doFixExpiredSaleTypeOpportunityData(List<Long> projectIds, Timestamp currentSyncTime) {
        if (!CollectionUtils.isEmpty(projectIds)) {
            String countTemplateSql = "SELECT\n"
                    + "   count(1)\n"
                    + "FROM\n"
                    + "   vendue_project ap\n"
                    + "WHERE ap.id in (%s) AND ap.vendue_end_time < ?";
            String queryTemplateSql = "SELECT\n" +
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
                    "                          ap.project_status AS projectStatus,\n" +
                    "                          ap.process_status AS processStatus,\n" +
                    "                          ap.node AS node,\n" +
                    "                          if(ap.vendue_real_stop_time is NULL,ap.vendue_end_time,ap.vendue_real_stop_time) AS quoteStopTime,\n" +
                    "                          ap.zone_str AS areaStr,\n" +
                    "                          ap.sys_id AS sourceId,\n" +
                    "                       ap.create_time AS createTime,\n" +
                    "                       ap.update_time AS updateTime\n" +
                    "                    FROM\n" +
                    "                       vendue_project ap\n" +
                    "                    WHERE\n" +
                    "                    ap.project_status > 2\n" +
                    "                     AND ap.id in (%s) AND ap.vendue_end_time < ?\n" +
                    "                     LIMIT ?,?\n" +
                    "                    ) s\n" +
                    "                 LEFT JOIN vendue_project_item api ON s.projectId = api.project_id\n" +
                    "                 AND s.purchaseId = api.company_id;";

            String countSql = String.format(countTemplateSql, StringUtils.collectionToCommaDelimitedString(projectIds));
            String querySql = String.format(queryTemplateSql, StringUtils.collectionToCommaDelimitedString(projectIds));
            doSyncProjectDataService(vendueDataSource, countSql, querySql, Collections.singletonList((Object) currentSyncTime), UPDATE_OPERATION);
        }
    }

    /**
     * 同步拍卖项目
     *
     * @param lastSyncTime
     */
    private void syncSaleProjectDataService(Timestamp lastSyncTime) {
        String countUpdatedSql = "SELECT\n" +
                "\tcount(1)\n" +
                "FROM\n" +
                "\tvendue_project \n" +
                "WHERE\n" +
                "\tproject_status > 2 AND update_time > ?";
        String queryUpdatedSql = "SELECT\n" +
                "\ts.*,\n" +
                "\tvpi.id AS directoryId,\n" +
                "\tvpi.`name` AS directoryName \n" +
                "FROM\n" +
                "\t(\n" +
                "SELECT\n" +
                "\tvp.company_id AS purchaseId,\n" +
                "\tvp.company_name AS purchaseName,\n" +
                "\tvp.id AS projectId,\n" +
                "\tvp.CODE AS projectCode,\n" +
                "\tvp.`name` AS projectName,\n" +
                "\tvp.province,\n" +
                "\tvp.project_status AS projectStatus,\n" +
                "\tvp.process_status AS processStatus,\n" +
                "\tvp.node AS node,\n" +
                "IF\n" +
                "\t( vp.vendue_real_stop_time IS NULL, vp.vendue_end_time, vp.vendue_real_stop_time ) AS quoteStopTime,\n" +
                "\tvp.zone_str AS areaStr,\n" +
                "\tvp.sys_id AS sourceId,\n" +
                "\tvp.create_time AS createTime,\n" +
                "\tvp.update_time AS updateTime \n" +
                "FROM\n" +
                "\tvendue_project vp \n" +
                "WHERE\n" +
                "\tvp.project_status > 2 \n" +
                "\tAND vp.update_time > ? \n" +
                "\tLIMIT ?,? \n" +
                "\t) s\n" +
                "\tLEFT JOIN vendue_project_item vpi ON s.projectId = vpi.project_id \n" +
                "\tAND s.purchaseId = vpi.company_id;";
        doSyncProjectDataService(vendueDataSource, countUpdatedSql, queryUpdatedSql, Collections.singletonList((Object) lastSyncTime), INSERT_OPERATION);
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
        // 项目拍卖中
        Timestamp quoteStopTime = (Timestamp) result.get(QUOTE_STOP_TIME);
        // 小于截止时间且待截标且拍卖中，则为商机，否则不是商机
        if (currentDate.before(quoteStopTime)) {
            result.put(STATUS, VALID_OPPORTUNITY_STATUS);
        } else {
            result.put(STATUS, INVALID_OPPORTUNITY_STATUS);
        }
        int projectStatus = (int) result.get(PROJECT_STATUS);
        if (Objects.equals(projectStatus, CANAL_PROJECT_STATUS)) {
            // 项目撤项 不展示
            result.put(IS_SHOW, HIDDEN);
        } else {
            result.put(IS_SHOW, SHOW);
        }
        resultToExecute.add(appendIdToResult(result, BusinessConstant.IXIETONG_SOURCE));
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
        result.put(PROJECT_TYPE, SALE_PROJECT_TYPE);

        //添加平台来源
        result.put(BusinessConstant.PLATFORM_SOURCE_KEY, BusinessConstant.IXIETONG_SOURCE);
    }


//    @Override
//    public void afterPropertiesSet() throws Exception {
//        execute();
//    }
}
