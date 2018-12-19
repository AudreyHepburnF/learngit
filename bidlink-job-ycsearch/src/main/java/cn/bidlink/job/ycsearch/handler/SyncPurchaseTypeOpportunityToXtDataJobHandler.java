package cn.bidlink.job.ycsearch.handler;

import cn.bidlink.job.common.constant.BusinessConstant;
import cn.bidlink.job.common.utils.AreaUtil;
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

import static cn.bidlink.job.common.utils.AreaUtil.queryAreaInfo;


/**
 * @author : <a href="mailto:zhihuizhou@ebnew.com">周治慧</a>
 * @version : Ver 1.0
 * @description :同步采购商机数据 悦采平台到隆道云 es中
 * @date : 2018/09/03
 */
@JobHander(value = "syncPurchaseTypeOpportunityToXtDataJobHandler")
@Service
public class SyncPurchaseTypeOpportunityToXtDataJobHandler extends AbstractSyncYcOpportunityDataJobHandler /*implements InitializingBean*/ {
    // 自动截标
    private int AUTO_STOP_TYPE   = 2;
    // 手动截标
    private int MANUAL_STOP_TYPE = 1;
    // 项目状态 撤项
    private int CANAL            = 10;

    private String BID_STOP_TYPE      = "bidStopType";
    private String BID_STOP_TIME      = "bidStopTime";
    private String BID_TRUE_STOP_TIME = "bidTrueStopTime";

    @Override
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
                QueryBuilders.boolQuery()
                        .must(QueryBuilders.termQuery("projectType", PURCHASE_PROJECT_TYPE))
                        .must(QueryBuilders.termQuery(BusinessConstant.PLATFORM_SOURCE_KEY, BusinessConstant.YUECAI_SOURCE)));
        Timestamp lastSyncStartTime = new Timestamp(new DateTime(new DateTime().getYear(), 1, 1, 0, 0, 0).getMillis());
        if (Objects.equals(SyncTimeUtil.GMT_TIME, lastSyncTime)) {
            lastSyncTime = lastSyncStartTime;
        }
//        DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
//        Timestamp lastSyncTime = new Timestamp(DateTime.parse("2018-12-18 15:20:10", dateTimeFormatter).getMillis());
        logger.info("采购项目商机同步时间,lastSyncTime：" + new DateTime(lastSyncTime).toString(SyncTimeUtil.DATE_TIME_PATTERN) + "\n"
                + ",syncTime:" + new DateTime(SyncTimeUtil.getCurrentDate()).toString(SyncTimeUtil.DATE_TIME_PATTERN));
        syncPurchaseProjectDataService(lastSyncTime);
        fixExpiredAutoStopTypePurchaseProjectDataService(lastSyncTime);
    }

    /**
     * 修复自动截标的数据无法同步的问题(自动截标数据需要每次拿同步时间小于当前时间数据 即之前同步过的自动截标数据在同步的时候有效,当前可能无效)
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
                .must(QueryBuilders.rangeQuery("syncTime").lt(currentTime))
                .must(QueryBuilders.termQuery(BusinessConstant.PLATFORM_SOURCE_KEY, BusinessConstant.YUECAI_SOURCE));

        int batchSize = 100;
        SearchResponse scrollResp = elasticClient.getTransportClient().prepareSearch(properties.getProperty("cluster.index"))
                .setTypes(properties.getProperty("cluster.type.supplier_opportunity"))
                .setQuery(queryBuilder)
                .setScroll(new TimeValue(60000))
                .setSize(batchSize)
                .get();

        do {
            SearchHit[] searchHits = scrollResp.getHits().hits();
            List<Long> projectIds = new ArrayList<>();
            for (SearchHit searchHit : searchHits) {
                projectIds.add(Long.valueOf(String.valueOf(searchHit.getSource().get(PROJECT_ID))));
            }
            doFixExpiredAutoStopTypePurchaseProjectDataService(projectIds, SyncTimeUtil.getCurrentDate());
            scrollResp = elasticClient.getTransportClient().prepareSearchScroll(scrollResp.getScrollId())
                    .setScroll(new TimeValue(60000))
                    .execute().actionGet();
        } while (scrollResp.getHits().getHits().length != 0);
        logger.info("修复自动截标商机结束");
    }

    private void doFixExpiredAutoStopTypePurchaseProjectDataService(List<Long> projectIds, Timestamp currentDate) {
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
                    + "   ( SELECT\n"
                    + "   bp.comp_id AS purchaseId,\n"
                    + "   bp.comp_name AS purchaseName,\n"
                    + "   bp.id AS projectId,\n"
                    + "   bp.`code` AS projectCode,\n"
                    + "   bp.`name` AS projectName,\n"
                    + "   bpe.purchase_open_range_type AS openRangeType,\n"
                    + "   IFNULL(bp.is_core,0) AS isCore,\n"
                    + "   bp.project_status AS projectStatus,\n"
                    + "   bpe.bid_stop_type AS bidStopType,\n"
                    + "   bpe.bid_stop_time AS bidStopTime,\n"
                    + "   bpe.bid_true_stop_time AS bidTrueStopTime,\n"
                    //                               + "   bpe.area_str AS areaStr,\n"
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
            doSyncProjectDataService(ycDataSource, countSql, querySql, Collections.singletonList((Object) currentDate));
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
        String queryUpdatedSql = "SELECT b.*, bpi.id AS directoryId, bpi.`name` AS directoryName FROM (SELECT\n"
                + "   bp.comp_id AS purchaseId,\n"
                + "   bp.comp_name AS purchaseName,\n"
                + "   bp.id AS projectId,\n"
                + "   bp.`code` AS projectCode,\n"
                + "   bp.`name` AS projectName,\n"
                + "   bpe.purchase_open_range_type AS openRangeType,\n"
                + "   IFNULL(bp.is_core,0) AS isCore,\n"
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
        doSyncProjectDataService(ycDataSource, countUpdatedSql, queryUpdatedSql, Collections.singletonList((Object) lastSyncTime));
    }

    @Override
    protected void appendAreaStrToResult(List<Map<String, Object>> resultToExecute, Set<Long> purchaseIds) {
        if (purchaseIds.size() > 0) {
            Map<Long, AreaUtil.AreaInfo> areaMap = queryAreaInfo(uniregDataSource, purchaseIds);
            for (Map<String, Object> result : resultToExecute) {
                Long purchaseId = Long.valueOf(String.valueOf(result.get(PURCHASE_ID)));
                AreaUtil.AreaInfo areaInfo = areaMap.get(purchaseId);
                if (areaInfo != null) {
                    result.put(AREA_STR, areaInfo.getAreaStr());
                    // 添加不分词的areaStr
                    result.put(AREA_STR_NOT_ANALYZED, result.get(AREA_STR));
                    result.put(REGION, areaInfo.getRegion());
                }
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
    @Override
    protected void parseOpportunity(Timestamp currentDate, List<Map<String, Object>> resultToExecute, Map<String, Object> result) {
        if (result.get(PROJECT_STATUS) == null || result.get(BID_STOP_TYPE) == null) {
            return;
        }
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


    @Override
    protected void refresh(Map<String, Object> result, Map<Long, Set<DirectoryEntity>> projectDirectoryMap) {
        super.refresh(result, projectDirectoryMap);
        // 移除不需要的属性
//        result.remove(BID_STOP_TYPE);
        result.put(QUOTE_STOP_TIME, SyncTimeUtil.toDateString(result.get(BID_STOP_TIME)));
        result.remove(BID_STOP_TIME);
        result.remove(BID_TRUE_STOP_TIME);
        // 项目类型
        result.put(PROJECT_TYPE, PURCHASE_PROJECT_TYPE);
        // 老平台
        result.put(SOURCE, SOURCE_OLD);
        // 是否展示(project_status=10 代表撤项,不展示)
        if (Objects.equals(result.get(PROJECT_STATUS), CANAL)) {
            result.put(IS_SHOW, HIDDEN);
        } else {
            result.put(IS_SHOW, SHOW);
        }
        // 悦采
        result.put(BusinessConstant.PLATFORM_SOURCE_KEY, BusinessConstant.YUECAI_SOURCE);
    }


//    @Override
//    public void afterPropertiesSet() throws Exception {
//        execute();
//    }
}
