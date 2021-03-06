package cn.bidlink.job.ycsearch.handler;

import cn.bidlink.job.common.constant.BusinessConstant;
import cn.bidlink.job.common.utils.ElasticClientUtil;
import cn.bidlink.job.common.utils.SyncTimeUtil;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.annotation.JobHander;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.joda.time.DateTime;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.sql.Timestamp;
import java.util.*;

/**
 * @author <a href="mailto:zhihuizhou@ebnew.com">zhouzhihui</a>
 * @version Ver 1.0
 * @description:
 * @Date 2018/9/6
 */
@Service
@JobHander(value = "syncAuctionTypeOpportunityToXtDataJobHandler")
public class SyncAuctionTypeOpportunityToXtDataJobHandler extends AbstractSyncYcOpportunityDataJobHandler /*implements InitializingBean*/ {

    // 待归档
    private int WAIT_ARCHIVE = 4;

    // 撤项
    private int CANAL = 6;

    private String QUOTE_STOP_TIME = "quoteStopTime";

    @Override
    public ReturnT<String> execute(String... strings) throws Exception {
        SyncTimeUtil.setCurrentDate();
        logger.info("开始同步悦采竞价项目商机到隆道云es中");
        syncAuctionProjectData();
        logger.info("结束同步悦采竞价项目商机到隆道云es中");
        return ReturnT.SUCCESS;
    }

    private void syncAuctionProjectData() {
        Timestamp lastSyncTime = ElasticClientUtil.getMaxTimestamp(elasticClient, "cluster.supplier_opportunity_index", "cluster.type.supplier_opportunity",
                QueryBuilders.boolQuery().must(QueryBuilders.termQuery(BusinessConstant.PLATFORM_SOURCE_KEY, BusinessConstant.YUECAI_SOURCE))
                        .must(QueryBuilders.termQuery(PROJECT_TYPE, AUCTION_PROJECT_TYPE)));
        Timestamp lastSyncStartTime = new Timestamp(new DateTime(new DateTime().getYear() - 1, 1, 1, 0, 0, 0).getMillis());
        if (Objects.equals(SyncTimeUtil.GMT_TIME, lastSyncTime)) {
            lastSyncTime = lastSyncStartTime;
        }
        logger.info("开始同步悦采竞价项目lastSyncTime:" + SyncTimeUtil.toDateString(lastSyncTime) + ",\n" + "syncTime:" + SyncTimeUtil.toDateString(SyncTimeUtil.getCurrentDate()));
        syncAuctionProjectDataService(lastSyncTime);
        // 修复竞价商机自动截止问题
//        fixExpiredYcAuctionTypeOpportunityData();
    }

    private void fixExpiredYcAuctionTypeOpportunityData() {
        logger.info("2.开始修复竞价项目自动截止商机状态");
        Properties properties = elasticClient.getProperties();
        int batchSize = 100;
        SearchResponse response = elasticClient.getTransportClient().prepareSearch(properties.getProperty("cluster.supplier_opportunity_index"))
                .setTypes(properties.getProperty("cluster.type.supplier_opportunity"))
                .setQuery(QueryBuilders.boolQuery()
                        .must(QueryBuilders.termQuery(BusinessConstant.PLATFORM_SOURCE_KEY, BusinessConstant.YUECAI_SOURCE))
                        .must(QueryBuilders.termQuery(PROJECT_TYPE, AUCTION_PROJECT_TYPE))
                ).setScroll(new TimeValue(60000))
                .setSize(batchSize)
                .setFetchSource(new String[]{PROJECT_ID}, null)
                .execute().actionGet();
        do {
            SearchHits hits = response.getHits();
            ArrayList<Long> projectIds = new ArrayList<>();
            for (SearchHit hit : hits.getHits()) {
                Map<String, Object> source = hit.getSourceAsMap();
                projectIds.add(Long.valueOf(source.get(PROJECT_ID).toString()));
            }
            doFixExpiredYcAuctionTypeOpportunityService(projectIds);

            response = elasticClient.getTransportClient().prepareSearchScroll(response.getScrollId())
                    .setScroll(new TimeValue(60000))
                    .execute().actionGet();
        } while (response.getHits().getHits().length != 0);

    }

    private void doFixExpiredYcAuctionTypeOpportunityService(ArrayList<Long> projectIds) {
        if (!CollectionUtils.isEmpty(projectIds)) {
            String countSqlTemplate = "SELECT\n"
                    + "\tcount( 1 ) \n"
                    + "FROM\n"
                    + "\tauction_project \n"
                    + "WHERE\n"
                    + "\tid in (%s)";
            String querySqlTemplate = "SELECT\n" +
                    "\tp.*,\n" +
                    "\tadi.id AS directoryId,\n" +
                    "\tadi.directory_name AS directoryName \n" +
                    "FROM\n" +
                    "\t(\n" +
                    "SELECT\n" +
                    "\tap.comp_id AS purchaseId,\n" +
                    "-- \tap. AS purchaseName,\n" +
                    "\tap.id AS projectId,\n" +
                    "\tap.project_code AS projectCode,\n" +
                    "\tap.project_name AS projectName,\n" +
                    "\tap.project_stats AS projectStatus,\n" +
                    "\tap.create_time AS createTime,\n" +
                    "\tap.update_time AS updateTime,\n" +
                    "\tar.auction_end_time As quoteStopTime\n" +
                    "FROM\n" +
                    "\tauction_project ap\n" +
                    "\tLEFT JOIN auction_rule ar ON ap.id = ar.project_id \n" +
                    "WHERE\n" +
                    "\tap.project_open_type = 1 \n" +
                    "\tAND ap.id in (%s) \n" +
                    "\tLIMIT ?,? \n" +
                    "\t) p\n" +
                    "\tJOIN auction_directory_info adi ON p.projectId = adi.project_id \n" +
                    "ORDER BY\n" +
                    "\tadi.id";
            String projectIdsStr = StringUtils.collectionToCommaDelimitedString(projectIds);
            String countSql = String.format(countSqlTemplate, projectIdsStr);
            String querySql = String.format(querySqlTemplate, projectIdsStr);
            doSyncProjectDataService(ycDataSource, countSql, querySql, Collections.emptyList());
        }
    }

    private void syncAuctionProjectDataService(Timestamp lastSyncTime) {
        doSyncInsertAuctionProjectData(lastSyncTime);
        doSyncUpdateAuctionProjectData(lastSyncTime);
    }

    private void doSyncInsertAuctionProjectData(Timestamp lastSyncTime) {
        String countSql = "SELECT\n"
                + "\tcount( 1 ) \n"
                + "FROM\n"
                + "\tauction_project \n"
                + "WHERE\n"
                + "\tcreate_time > ?";
        String querySql = "SELECT\n" +
                "\tp.*,\n" +
                "\tadi.id AS directoryId,\n" +
                "\tadi.directory_name AS directoryName \n" +
                "FROM\n" +
                "\t(\n" +
                "SELECT\n" +
                "\tap.comp_id AS purchaseId,\n" +
                "-- \tap. AS purchaseName,\n" +
                "\tap.id AS projectId,\n" +
                "\tap.project_code AS projectCode,\n" +
                "\tap.project_name AS projectName,\n" +
                "\tap.project_stats AS projectStatus,\n" +
                "\tap.create_time AS createTime,\n" +
                "\tap.update_time AS updateTime,\n" +
                "\tar.auction_end_time As quoteStopTime\n" +
                "FROM\n" +
                "\tauction_project ap\n" +
                "\tLEFT JOIN auction_rule ar ON ap.id = ar.project_id \n" +
                "WHERE\n" +
                "\tap.project_open_type = 1 \n" +
                "\tAND ap.create_time > ? \n" +
                "\tLIMIT ?,? \n" +
                "\t) p\n" +
                "\tJOIN auction_directory_info adi ON p.projectId = adi.project_id \n" +
                "ORDER BY\n" +
                "\tadi.id";
        doSyncProjectDataService(ycDataSource, countSql, querySql, Collections.singletonList(lastSyncTime));
    }

    private void doSyncUpdateAuctionProjectData(Timestamp lastSyncTime) {
        String countSql = "SELECT\n"
                + "\tcount( 1 ) \n"
                + "FROM\n"
                + "\tauction_project \n"
                + "WHERE\n"
                + "\tupdate_time > ?";
        String querySql = "SELECT\n" +
                "\tp.*,\n" +
                "\tadi.id AS directoryId,\n" +
                "\tadi.directory_name AS directoryName \n" +
                "FROM\n" +
                "\t(\n" +
                "SELECT\n" +
                "\tap.comp_id AS purchaseId,\n" +
                "-- \tap. AS purchaseName,\n" +
                "\tap.id AS projectId,\n" +
                "\tap.project_code AS projectCode,\n" +
                "\tap.project_name AS projectName,\n" +
                "\tap.project_stats AS projectStatus,\n" +
                "\tap.create_time AS createTime,\n" +
                "\tap.update_time AS updateTime,\n" +
                "\tar.auction_end_time As quoteStopTime\n" +
                "FROM\n" +
                "\tauction_project ap\n" +
                "\tLEFT JOIN auction_rule ar ON ap.id = ar.project_id \n" +
                "WHERE\n" +
                "\tap.project_open_type = 1 \n" +
                "\tAND ap.update_time > ? \n" +
                "\tLIMIT ?,? \n" +
                "\t) p\n" +
                "\tJOIN auction_directory_info adi ON p.projectId = adi.project_id \n" +
                "ORDER BY\n" +
                "\tadi.id";
        doSyncProjectDataService(ycDataSource, countSql, querySql, Collections.singletonList(lastSyncTime));
    }

    @Override
    protected void parseOpportunity(Timestamp currentDate, List<Map<String, Object>> resultToExecute, Map<String, Object> result) {
        Integer projectStatus = Integer.valueOf(result.get(PROJECT_STATUS).toString());
        Timestamp quoteStopTime = (Timestamp) result.get(QUOTE_STOP_TIME);
        if (projectStatus < WAIT_ARCHIVE && currentDate.before(quoteStopTime)) {
            result.put(STATUS, VALID_OPPORTUNITY_STATUS);
        } else {
            result.put(STATUS, INVALID_OPPORTUNITY_STATUS);
        }
        if (projectStatus == CANAL) {
            // 撤项
            result.put(IS_SHOW, HIDDEN);
        } else {
            result.put(IS_SHOW, SHOW);
        }
        resultToExecute.add(appendIdToResult(result));
    }

    @Override
    protected void refresh(Map<String, Object> result, Map<Long, Set<DirectoryEntity>> projectDirectoryMap) {
        super.refresh(result, projectDirectoryMap);
        result.put(PROJECT_TYPE, AUCTION_PROJECT_TYPE);
        result.put(BusinessConstant.PLATFORM_SOURCE_KEY, BusinessConstant.YUECAI_SOURCE);
    }

//    @Override
//    public void afterPropertiesSet() throws Exception {
//        execute();
//    }
}
