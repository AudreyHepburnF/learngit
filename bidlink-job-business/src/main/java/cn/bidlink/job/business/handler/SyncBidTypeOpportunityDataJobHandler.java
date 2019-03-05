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
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.sql.Timestamp;
import java.util.*;


/**
 * 同步协同平台招标商机数据
 *
 * @author : <a href="mailto:zikaifeng@ebnew.com">冯子恺</a>
 * @version : Ver 1.0
 * @description :
 * @date : 2017/8/7
 */
@JobHander(value = "syncBidTypeOpportunityDataJobHandler")
@Service
public class SyncBidTypeOpportunityDataJobHandler extends AbstractSyncOpportunityDataJobHandler implements InitializingBean {

    private String OPEN_RANGE_TYPE = "openRangeType";
    private String NODE            = "node";

    @Override
    public ReturnT<String> execute(String... strings) throws Exception {
        SyncTimeUtil.setCurrentDate();
        logger.info("同步协同招标项目的商机开始");
        syncOpportunityData();
        logger.info("同步协同招标项目的商机结束");
        return ReturnT.SUCCESS;
    }

    /**
     * 同步招标商机
     */
    private void syncOpportunityData() {
        Timestamp lastSyncTime = ElasticClientUtil.getMaxTimestamp(elasticClient,
                "cluster.index",
                "cluster.type.supplier_opportunity",
                QueryBuilders.boolQuery()
                        .must(QueryBuilders.termQuery(PROJECT_TYPE, BIDDING_PROJECT_TYPE))
                        .must(QueryBuilders.termQuery(BusinessConstant.PLATFORM_SOURCE_KEY, BusinessConstant.IXIETONG_SOURCE)));
        logger.info("招标项目商机同步 lastSyncTime:" + SyncTimeUtil.toDateString(lastSyncTime), ",\n syncTime:" + SyncTimeUtil.currentDateToString());
//        Timestamp lastSyncTime = new Timestamp(SyncTimeUtil.toStringDate("2019-01-18 14:50:00").getTime());
        syncBiddingProjectDataService(lastSyncTime);
        // 修复招标项目 截止时间到后,商机状态
        fixExpiredBiddingProjectDataService();
    }

    /**
     * 修复招标项目商机状态
     */
    private void fixExpiredBiddingProjectDataService() {
        logger.info("开始修复招标项目自动截标商机状态");
//        long startTime = System.currentTimeMillis();
        String currentDate = SyncTimeUtil.toDateString(SyncTimeUtil.getCurrentDate());
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.termQuery(BusinessConstant.PLATFORM_SOURCE_KEY, BusinessConstant.IXIETONG_SOURCE))
                .must(QueryBuilders.termQuery(PROJECT_TYPE, BIDDING_PROJECT_TYPE))
                .must(QueryBuilders.rangeQuery(SyncTimeUtil.SYNC_TIME).lte(currentDate));
        int batchSize = 100;
        Properties properties = elasticClient.getProperties();
        SearchResponse scrollResp = elasticClient.getTransportClient().prepareSearch(properties.getProperty("cluster.index"))
                .setTypes(properties.getProperty("cluster.type.supplier_opportunity"))
                .setQuery(queryBuilder)
                .setScroll(new TimeValue(60000))
                .setFetchSource(new String[]{PROJECT_ID}, null)
                .setSize(batchSize)
                .execute().actionGet();
        do {
            SearchHit[] hits = scrollResp.getHits().getHits();
            List<Long> projectIds = new ArrayList<>();
            for (SearchHit hit : hits) {
                projectIds.add(Long.valueOf(hit.getSource().get("projectId").toString()));
            }
            doFixExpiredBiddingProjectDataService(projectIds);
            scrollResp = elasticClient.getTransportClient().prepareSearchScroll(scrollResp.getScrollId())
                    .setScroll(new TimeValue(60000))
                    .execute().actionGet();
        } while (scrollResp.getHits().getHits().length != 0);
//        long endTime = System.currentTimeMillis();
//        System.out.println(endTime - startTime);
        logger.info("结束修复招标项目自动截标商机状态");
    }

    private void doFixExpiredBiddingProjectDataService(List<Long> projectIds) {
        if (!CollectionUtils.isEmpty(projectIds)) {
            String countTemplateSql = "SELECT\n"
                    + "   count(1)\n"
                    + "FROM\n"
                    + "   bid_sub_project\n"
                    + "WHERE\n"
                    + "   is_bid_open = 1 AND approve_status = 2 AND  id in (%s) AND bid_endtime < ?";
            String queryTemplateSql = "SELECT\n" +
                    "\tproject.*,\n" +
                    "\tbpi.id AS directoryId,\n" +
                    "\tbpi.`name` AS directoryName \n" +
                    "FROM\n" +
                    "\t(\n" +
                    "SELECT\n" +
                    "\tbsp.id AS projectId,\n" +
                    "\tbsp.project_code AS projectCode,\n" +
                    "\tbsp.project_name AS projectName,\n" +
                    "\tbsp.project_status AS projectStatus,\n" +
                    "\tbsp.company_id AS purchaseId,\n" +
                    "\tbsp.company_name AS purchaseName,\n" +
                    "\tbsp.create_time AS createTime,\n" +
                    "\tcase \n" +
                    "\tWHEN bsp.project_type in (1,2,3) THEN\n" +
                    "\t\tbsp.node\n" +
                    "\tELSE\n" +
                    "\t\tbsp.negotiation_node\n" +
                    "END as node,\n" +
                    "\tbsp.bid_open_time AS bidOpenTime,\n" +
                    "\tbsp.bid_endtime AS quoteStopTime,\n" +
                    "\tbsp.sys_id AS sourceId,\n" +
                    "\tbsp.update_time AS updateTime,\n" +
                    "\tbp.province,\n" +
                    "\tbp.zone_str AS areaStr, \n" +
                    "\tbp.industry_name AS industryStr \n" +
                    "FROM\n" +
                    "\tbid_sub_project bsp\n" +
                    "\tLEFT JOIN bid_project bp ON bsp.project_id = bp.id \n" +
                    "\tAND bsp.company_id = bp.company_id \n" +
                    "WHERE\n" +
                    "\t bsp.id in (%s) \n" +
                    "\tAND bsp.bid_endtime < ? \n" +
                    "\tLIMIT ?,? \n" +
                    "\t) project\n" +
                    "\tLEFT JOIN bid_project_item bpi ON project.projectId = bpi.sub_project_id";
            String projectIdsStr = StringUtils.collectionToCommaDelimitedString(projectIds);
            String countSql = String.format(countTemplateSql, projectIdsStr);
            String querySql = String.format(queryTemplateSql, projectIdsStr);
            doSyncProjectDataService(tenderDataSource, countSql, querySql, Collections.singletonList(SyncTimeUtil.getCurrentDate()), UPDATE_OPERATION);
        }
    }

    /**
     * 同步招标项目  bid_project_type  11:采购商招标 21:机构招标资格预审 22:机构招标无资格预审 32:竞争性谈判 42:竞争性磋商
     *
     * @param lastSyncTime
     */
    private void syncBiddingProjectDataService(Timestamp lastSyncTime) {
        // 采购商版本
        syncCGBiddingProjectDataService(lastSyncTime);
        // 机构版招标项目
        // 1.公开无资格预审
        syncJGBiddingOpenNoPrequalificationProjectDataService(lastSyncTime);
        // 2.公开有资格预审,只同步资格预审的项目
        syncJGBiddingOpenPrequalificationProjectDataService(lastSyncTime);
        // 3.邀请招标
        syncJGBiddingInviteProjectDataService(lastSyncTime);
        // 4.竞争性谈判
        syncJGCompetitiveNegotiationsProjectDataService(lastSyncTime);
        // 5.竞争性磋商
        syncJGCompetitiveConsultationProjectDataService(lastSyncTime);
        // 6.单一性来源
        syncJGSingleSourceProjectDataService(lastSyncTime);
        // 7.询价
        syncJGEnquiryProjectDataService(lastSyncTime);

    }

    private void syncJGEnquiryProjectDataService(Timestamp lastSyncTime) {
        String countUpdatedSql = "SELECT\n"
                + "   count(1)\n"
                + "FROM\n"
                + "   bid_sub_project\n"
                + "WHERE\n"
                + "   is_bid_open = 1 AND negotiation_node > 1 AND approve_status = 2 and project_type = 6 AND update_time > ?";
        String queryUpdatedSql = "SELECT\n" +
                "\tproject.*,\n" +
                "\tbpi.id AS directoryId,\n" +
                "\tbpi.`name` AS directoryName \n" +
                "FROM\n" +
                "\t(\n" +
                "SELECT\n" +
                "\tbsp.id AS projectId,\n" +
                "\tbsp.project_code AS projectCode,\n" +
                "\tbsp.project_name AS projectName,\n" +
                "\tbsp.project_status AS projectStatus,\n" +
                "\tbsp.company_id AS purchaseId,\n" +
                "\tbsp.company_name AS purchaseName,\n" +
                "\tbsp.create_time AS createTime,\n" +
                "\tbsp.negotiation_node as node,\n" +
                "\t62 as bidProjectType,\n" +
                "\tbsp.bid_open_time AS bidOpenTime,\n" +
                "\tbsp.bid_endtime AS quoteStopTime,\n" +
                "\tbsp.sys_id AS sourceId,\n" +
                "\tbsp.update_time AS updateTime,\n" +
                "\tbp.province,\n" +
                "\tbp.zone_str AS areaStr, \n" +
                "\tbp.bid_type AS bidType, \n" +
                "\tbp.industry_name AS industryStr \n" +
                "FROM\n" +
                "\tbid_sub_project bsp\n" +
                "\tLEFT JOIN bid_project bp ON bsp.project_id = bp.id \n" +
                "\tAND bsp.company_id = bp.company_id \n" +
                "WHERE\n" +
                "\tis_bid_open = 1 \n" +
                "\tAND negotiation_node > 1 \n" +
                "\tAND bsp.approve_status = 2\n" +
                "\tAND bsp.project_type = 6\n" +
                "\tAND bsp.update_time > ? and bsp.bid_endtime is not null\n" +
                "\tLIMIT ?,? \n" +
                "\t) project\n" +
                "\tLEFT JOIN bid_project_item bpi ON project.projectId = bpi.sub_project_id";
        doSyncProjectDataService(tenderDataSource, countUpdatedSql, queryUpdatedSql, Collections.singletonList(((Object) lastSyncTime)), INSERT_OPERATION);
    }

    private void syncJGSingleSourceProjectDataService(Timestamp lastSyncTime) {
        String countUpdatedSql = "SELECT\n"
                + "   count(1)\n"
                + "FROM\n"
                + "   bid_sub_project\n"
                + "WHERE\n"
                + "   is_bid_open = 1 AND negotiation_node > 1 AND approve_status = 2 and project_type = 5 AND update_time > ?";
        String queryUpdatedSql = "SELECT\n" +
                "\tproject.*,\n" +
                "\tbpi.id AS directoryId,\n" +
                "\tbpi.`name` AS directoryName \n" +
                "FROM\n" +
                "\t(\n" +
                "SELECT\n" +
                "\tbsp.id AS projectId,\n" +
                "\tbsp.project_code AS projectCode,\n" +
                "\tbsp.project_name AS projectName,\n" +
                "\tbsp.project_status AS projectStatus,\n" +
                "\tbsp.company_id AS purchaseId,\n" +
                "\tbsp.company_name AS purchaseName,\n" +
                "\tbsp.create_time AS createTime,\n" +
                "\tbsp.negotiation_node as node,\n" +
                "\t52 as bidProjectType,\n" +
                "\tbsp.bid_open_time AS bidOpenTime,\n" +
                "\tbsp.bid_endtime AS quoteStopTime,\n" +
                "\tbsp.sys_id AS sourceId,\n" +
                "\tbsp.update_time AS updateTime,\n" +
                "\tbp.province,\n" +
                "\tbp.zone_str AS areaStr, \n" +
                "\tbp.bid_type AS bidType, \n" +
                "\tbp.industry_name AS industryStr \n" +
                "FROM\n" +
                "\tbid_sub_project bsp\n" +
                "\tLEFT JOIN bid_project bp ON bsp.project_id = bp.id \n" +
                "\tAND bsp.company_id = bp.company_id \n" +
                "WHERE\n" +
                "\tis_bid_open = 1 \n" +
                "\tAND negotiation_node > 1 \n" +
                "\tAND bsp.approve_status = 2\n" +
                "\tAND bsp.project_type = 5\n" +
                "\tAND bsp.update_time > ? and bsp.bid_endtime is not null\n" +
                "\tLIMIT ?,? \n" +
                "\t) project\n" +
                "\tLEFT JOIN bid_project_item bpi ON project.projectId = bpi.sub_project_id";
        doSyncProjectDataService(tenderDataSource, countUpdatedSql, queryUpdatedSql, Collections.singletonList(((Object) lastSyncTime)), INSERT_OPERATION);
    }

    private void syncJGCompetitiveConsultationProjectDataService(Timestamp lastSyncTime) {
        String countUpdatedSql = "SELECT\n"
                + "   count(1)\n"
                + "FROM\n"
                + "   bid_sub_project\n"
                + "WHERE\n"
                + "   is_bid_open = 1 AND negotiation_node > 1 AND approve_status = 2 and project_type = 4 AND update_time > ?";
        String queryUpdatedSql = "SELECT\n" +
                "\tproject.*,\n" +
                "\tbpi.id AS directoryId,\n" +
                "\tbpi.`name` AS directoryName \n" +
                "FROM\n" +
                "\t(\n" +
                "SELECT\n" +
                "\tbsp.id AS projectId,\n" +
                "\tbsp.project_code AS projectCode,\n" +
                "\tbsp.project_name AS projectName,\n" +
                "\tbsp.project_status AS projectStatus,\n" +
                "\tbsp.company_id AS purchaseId,\n" +
                "\tbsp.company_name AS purchaseName,\n" +
                "\tbsp.create_time AS createTime,\n" +
                "\tbsp.negotiation_node as node,\n" +
                "\t42 as bidProjectType,\n" +
                "\tbsp.bid_open_time AS bidOpenTime,\n" +
                "\tbsp.bid_endtime AS quoteStopTime,\n" +
                "\tbsp.sys_id AS sourceId,\n" +
                "\tbsp.update_time AS updateTime,\n" +
                "\tbp.province,\n" +
                "\tbp.zone_str AS areaStr, \n" +
                "\tbp.bid_type AS bidType, \n" +
                "\tbp.industry_name AS industryStr \n" +
                "FROM\n" +
                "\tbid_sub_project bsp\n" +
                "\tLEFT JOIN bid_project bp ON bsp.project_id = bp.id \n" +
                "\tAND bsp.company_id = bp.company_id \n" +
                "WHERE\n" +
                "\tis_bid_open = 1 \n" +
                "\tAND negotiation_node > 1 \n" +
                "\tAND bsp.approve_status = 2\n" +
                "\tAND bsp.project_type = 4\n" +
                "\tAND bsp.update_time > ? and bsp.bid_endtime is not null\n" +
                "\tLIMIT ?,? \n" +
                "\t) project\n" +
                "\tLEFT JOIN bid_project_item bpi ON project.projectId = bpi.sub_project_id";
        doSyncProjectDataService(tenderDataSource, countUpdatedSql, queryUpdatedSql, Collections.singletonList(((Object) lastSyncTime)), INSERT_OPERATION);
    }

    private void syncJGCompetitiveNegotiationsProjectDataService(Timestamp lastSyncTime) {
        String countUpdatedSql = "SELECT\n"
                + "   count(1)\n"
                + "FROM\n"
                + "   bid_sub_project\n"
                + "WHERE\n"
                + "   is_bid_open = 1 AND negotiation_node > 1 AND approve_status = 2 and project_type = 3 AND update_time > ?";
        String queryUpdatedSql = "SELECT\n" +
                "\tproject.*,\n" +
                "\tbpi.id AS directoryId,\n" +
                "\tbpi.`name` AS directoryName \n" +
                "FROM\n" +
                "\t(\n" +
                "SELECT\n" +
                "\tbsp.id AS projectId,\n" +
                "\tbsp.project_code AS projectCode,\n" +
                "\tbsp.project_name AS projectName,\n" +
                "\tbsp.project_status AS projectStatus,\n" +
                "\tbsp.company_id AS purchaseId,\n" +
                "\tbsp.company_name AS purchaseName,\n" +
                "\tbsp.create_time AS createTime,\n" +
                "\tbsp.negotiation_node as node,\n" +
                "\t32 as bidProjectType,\n" +
                "\tbsp.bid_open_time AS bidOpenTime,\n" +
                "\tbsp.bid_endtime AS quoteStopTime,\n" +
                "\tbsp.sys_id AS sourceId,\n" +
                "\tbsp.update_time AS updateTime,\n" +
                "\tbp.province,\n" +
                "\tbp.zone_str AS areaStr, \n" +
                "\tbp.bid_type AS bidType, \n" +
                "\tbp.industry_name AS industryStr \n" +
                "FROM\n" +
                "\tbid_sub_project bsp\n" +
                "\tLEFT JOIN bid_project bp ON bsp.project_id = bp.id \n" +
                "\tAND bsp.company_id = bp.company_id \n" +
                "WHERE\n" +
                "\tis_bid_open = 1 \n" +
                "\tAND negotiation_node > 1 \n" +
                "\tAND bsp.approve_status = 2\n" +
                "\tAND bsp.project_type = 3\n" +
                "\tAND bsp.update_time > ? and bsp.bid_endtime is not null\n" +
                "\tLIMIT ?,? \n" +
                "\t) project\n" +
                "\tLEFT JOIN bid_project_item bpi ON project.projectId = bpi.sub_project_id";
        doSyncProjectDataService(tenderDataSource, countUpdatedSql, queryUpdatedSql, Collections.singletonList(((Object) lastSyncTime)), INSERT_OPERATION);
    }

    private void syncJGBiddingInviteProjectDataService(Timestamp lastSyncTime) {
        String countUpdatedSql = "SELECT\n"
                + "   count(1)\n"
                + "FROM\n"
                + "   bid_sub_project\n"
                + "WHERE\n"
                + "   is_bid_open = 1 AND node > 1 AND approve_status = 2 and project_type = 2 AND bid_type = 2 AND update_time > ?";
        String queryUpdatedSql = "SELECT\n" +
                "\tproject.*,\n" +
                "\tbpi.id AS directoryId,\n" +
                "\tbpi.`name` AS directoryName \n" +
                "FROM\n" +
                "\t(\n" +
                "SELECT\n" +
                "\tbsp.id AS projectId,\n" +
                "\tbsp.project_code AS projectCode,\n" +
                "\tbsp.project_name AS projectName,\n" +
                "\tbsp.project_status AS projectStatus,\n" +
                "\tbsp.company_id AS purchaseId,\n" +
                "\tbsp.company_name AS purchaseName,\n" +
                "\tbsp.create_time AS createTime,\n" +
                "\tbsp.node,\n" +
                "\t22 as bidProjectType,\n" +
                "\tbsp.bid_open_time AS bidOpenTime,\n" +
                "\tbsp.bid_endtime AS quoteStopTime,\n" +
                "\tbsp.sys_id AS sourceId,\n" +
                "\tbsp.update_time AS updateTime,\n" +
                "\tbp.province,\n" +
                "\tbp.zone_str AS areaStr, \n" +
                "\tbp.bid_type AS bidType, \n" +
                "\tbp.industry_name AS industryStr \n" +
                "FROM\n" +
                "\tbid_sub_project bsp\n" +
                "\tLEFT JOIN bid_project bp ON bsp.project_id = bp.id \n" +
                "\tAND bsp.company_id = bp.company_id \n" +
                "WHERE\n" +
                "\tis_bid_open = 1 \n" +
                "\tAND node > 1 \n" +
                "\tAND bsp.approve_status = 2\n" +
                "\tAND bsp.bid_type = 2\n" +
                "\tAND bsp.project_type = 2\n" +
                "\tAND bsp.update_time > ? and bsp.bid_endtime is not null\n" +
                "\tLIMIT ?,? \n" +
                "\t) project\n" +
                "\tLEFT JOIN bid_project_item bpi ON project.projectId = bpi.sub_project_id";
        doSyncProjectDataService(tenderDataSource, countUpdatedSql, queryUpdatedSql, Collections.singletonList(((Object) lastSyncTime)), INSERT_OPERATION);
    }

    private void syncJGBiddingOpenPrequalificationProjectDataService(Timestamp lastSyncTime) {
        String countUpdatedSql = "SELECT\n"
                + "   count(1)\n"
                + "FROM\n"
                + "   bid_sub_project\n"
                + "WHERE\n"
                + "   is_bid_open = 1 AND node > 1 AND approve_status = 2 and project_type = 2 AND is_need_prequa =1 and is_prequa_project_data=1 AND bid_type = 1 AND update_time > ?";
        String queryUpdatedSql = "SELECT\n" +
                "\tproject.*,\n" +
                "\tbpi.id AS directoryId,\n" +
                "\tbpi.`name` AS directoryName \n" +
                "FROM\n" +
                "\t(\n" +
                "SELECT\n" +
                "\tbsp.id AS projectId,\n" +
                "\tbsp.project_code AS projectCode,\n" +
                "\tbsp.project_name AS projectName,\n" +
                "\tbsp.project_status AS projectStatus,\n" +
                "\tbsp.company_id AS purchaseId,\n" +
                "\tbsp.company_name AS purchaseName,\n" +
                "\tbsp.create_time AS createTime,\n" +
                "\tbsp.node,\n" +
                "\t21 as bidProjectType,\n" +
                "\tbsp.bid_open_time AS bidOpenTime,\n" +
                "\tbsp.bid_endtime AS quoteStopTime,\n" +
                "\tbsp.sys_id AS sourceId,\n" +
                "\tbsp.update_time AS updateTime,\n" +
                "\tbp.province,\n" +
                "\tbp.zone_str AS areaStr, \n" +
                "\tbp.bid_type AS bidType, \n" +
                "\tbp.industry_name AS industryStr \n" +
                "FROM\n" +
                "\tbid_sub_project bsp\n" +
                "\tLEFT JOIN bid_project bp ON bsp.project_id = bp.id \n" +
                "\tAND bsp.company_id = bp.company_id \n" +
                "WHERE\n" +
                "\tis_bid_open = 1 \n" +
                "\tAND node > 1 \n" +
                "\tAND bsp.approve_status = 2\n" +
                "\tAND bsp.project_type = 2\n" +
                "\tAND bsp.bid_type = 1\n" +
                "\tAND bsp.is_need_prequa =1\n" +
                "\tAND bsp.is_prequa_project_data =1\n" +
                "\tAND bsp.update_time > ? and bsp.bid_endtime is not null\n" +
                "\tLIMIT ?,? \n" +
                "\t) project\n" +
                "\tLEFT JOIN bid_project_item bpi ON project.projectId = bpi.sub_project_id";
        doSyncProjectDataService(tenderDataSource, countUpdatedSql, queryUpdatedSql, Collections.singletonList(((Object) lastSyncTime)), INSERT_OPERATION);
    }

    private void syncJGBiddingOpenNoPrequalificationProjectDataService(Timestamp lastSyncTime) {
        String countUpdatedSql = "SELECT\n"
                + "   count(1)\n"
                + "FROM\n"
                + "   bid_sub_project\n"
                + "WHERE\n"
                + "   is_bid_open = 1 AND node > 1 AND bid_type = 1 and approve_status = 2 AND project_type = 2 AND is_need_prequa =2 AND update_time > ?";
        String queryUpdatedSql = "SELECT\n" +
                "\tproject.*,\n" +
                "\tbpi.id AS directoryId,\n" +
                "\tbpi.`name` AS directoryName \n" +
                "FROM\n" +
                "\t(\n" +
                "SELECT\n" +
                "\tbsp.id AS projectId,\n" +
                "\tbsp.project_code AS projectCode,\n" +
                "\tbsp.project_name AS projectName,\n" +
                "\tbsp.project_status AS projectStatus,\n" +
                "\tbsp.company_id AS purchaseId,\n" +
                "\tbsp.company_name AS purchaseName,\n" +
                "\tbsp.create_time AS createTime,\n" +
                "\tbsp.node,\n" +
                "\t22 as bidProjectType,\n" +
                "\tbsp.bid_open_time AS bidOpenTime,\n" +
                "\tbsp.bid_endtime AS quoteStopTime,\n" +
                "\tbsp.sys_id AS sourceId,\n" +
                "\tbsp.update_time AS updateTime,\n" +
                "\tbp.province,\n" +
                "\tbp.zone_str AS areaStr, \n" +
                "\tbp.bid_type AS bidType, \n" +
                "\tbp.industry_name AS industryStr \n" +
                "FROM\n" +
                "\tbid_sub_project bsp\n" +
                "\tLEFT JOIN bid_project bp ON bsp.project_id = bp.id \n" +
                "\tAND bsp.company_id = bp.company_id \n" +
                "WHERE\n" +
                "\tis_bid_open = 1 \n" +
                "\tAND node > 1 \n" +
                "\tAND bsp.approve_status = 2\n" +
                "\tAND bsp.bid_type = 1\n" +
                "\tAND bsp.project_type = 2\n" +
                "\tAND bsp.is_need_prequa =2\n" +
                "\tAND bsp.update_time > ? and bsp.bid_endtime is not null\n" +
                "\tLIMIT ?,? \n" +
                "\t) project\n" +
                "\tLEFT JOIN bid_project_item bpi ON project.projectId = bpi.sub_project_id";
        doSyncProjectDataService(tenderDataSource, countUpdatedSql, queryUpdatedSql, Collections.singletonList(((Object) lastSyncTime)), INSERT_OPERATION);
    }

    private void syncCGBiddingProjectDataService(Timestamp lastSyncTime) {
        String countUpdatedSql = "SELECT\n"
                + "   count(1)\n"
                + "FROM\n"
                + "   bid_sub_project\n"
                + "WHERE\n"
                + "   is_bid_open = 1 AND node > 1 AND approve_status = 2 and project_type = 1 AND update_time > ?";
        String queryUpdatedSql = "SELECT\n" +
                "\tproject.*,\n" +
                "\tbpi.id AS directoryId,\n" +
                "\tbpi.`name` AS directoryName \n" +
                "FROM\n" +
                "\t(\n" +
                "SELECT\n" +
                "\tbsp.id AS projectId,\n" +
                "\tbsp.project_code AS projectCode,\n" +
                "\tbsp.project_name AS projectName,\n" +
                "\tbsp.project_status AS projectStatus,\n" +
                "\tbsp.company_id AS purchaseId,\n" +
                "\tbsp.company_name AS purchaseName,\n" +
                "\tbsp.create_time AS createTime,\n" +
                "\tbsp.node,\n" +
                "\t11 as bidProjectType,\n" +
                "\tbsp.bid_open_time AS bidOpenTime,\n" +
                "\tbsp.bid_endtime AS quoteStopTime,\n" +
                "\tbsp.sys_id AS sourceId,\n" +
                "\tbsp.update_time AS updateTime,\n" +
                "\tbp.province,\n" +
                "\tbp.zone_str AS areaStr, \n" +
                "\tbp.bid_type AS bidType, \n" +
                "\tbp.industry_name AS industryStr \n" +
                "FROM\n" +
                "\tbid_sub_project bsp\n" +
                "\tLEFT JOIN bid_project bp ON bsp.project_id = bp.id \n" +
                "\tAND bsp.company_id = bp.company_id \n" +
                "WHERE\n" +
                "\tis_bid_open = 1 \n" +
                "\tAND node > 1 \n" +
                "\tAND bsp.approve_status = 2\n" +
                "\tAND bsp.project_type = 1\n" +
                "\tAND bsp.update_time > ? and bsp.bid_endtime is not null\n" +
                "\tLIMIT ?,? \n" +
                "\t) project\n" +
                "\tLEFT JOIN bid_project_item bpi ON project.projectId = bpi.sub_project_id";
        doSyncProjectDataService(tenderDataSource, countUpdatedSql, queryUpdatedSql, Collections.singletonList(((Object) lastSyncTime)), INSERT_OPERATION);
    }

    @Override
    protected void refresh(Map<String, Object> result, Map<Long, Set<DirectoryEntity>> projectDirectoryMap) {
        super.refresh(result, projectDirectoryMap);
        result.put(QUOTE_STOP_TIME, SyncTimeUtil.toDateString(result.get(QUOTE_STOP_TIME)));
        // 项目类型
        result.put(PROJECT_TYPE, BIDDING_PROJECT_TYPE);
        // 公开类型，默认为1
        result.put(OPEN_RANGE_TYPE, 1);
        //添加平台来源
        result.put(BusinessConstant.PLATFORM_SOURCE_KEY, BusinessConstant.IXIETONG_SOURCE);
    }

    @Override
    protected void parseOpportunity(Timestamp currentDate, List<Map<String, Object>> resultToExecute, Map<String, Object> result) {
        int PROJECT_EXECUTING = 1;  // 项目正在进行中
        int BIDDING = 2;            // 投标阶段
        Integer node = (Integer) result.get(NODE);
        Integer projectStatus = (Integer) result.get(PROJECT_STATUS);
        Timestamp quoteStopTime = (Timestamp) result.get(QUOTE_STOP_TIME);
        // 小于截止时间且项目正在进行中且节点是投标阶段，则为商机，否则不是商机
        if (currentDate.before(quoteStopTime)
                && projectStatus == PROJECT_EXECUTING
                && node == BIDDING) {
            result.put(STATUS, VALID_OPPORTUNITY_STATUS);
        } else {
            result.put(STATUS, INVALID_OPPORTUNITY_STATUS);
        }
        // 招标商机默认展示
        result.put(IS_SHOW, SHOW);
        resultToExecute.add(appendIdToResult(result, BusinessConstant.IXIETONG_SOURCE));
    }


    @Override
    public void afterPropertiesSet() throws Exception {
        execute();
    }
}
