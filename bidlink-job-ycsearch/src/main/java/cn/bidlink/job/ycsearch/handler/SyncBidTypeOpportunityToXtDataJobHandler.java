package cn.bidlink.job.ycsearch.handler;

import cn.bidlink.job.common.constant.BusinessConstant;
import cn.bidlink.job.common.utils.ElasticClientUtil;
import cn.bidlink.job.common.utils.SyncTimeUtil;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.annotation.JobHander;
import org.elasticsearch.index.query.QueryBuilders;
import org.joda.time.DateTime;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.util.*;


/**
 * @author : <a href="mailto:zhihuizhou@ebnew.com">周治慧</a>
 * @version : Ver 1.0
 * @description :同步招标商机数据 悦采平台到隆道云 es中
 * @date : 2018/09/03
 */
@JobHander(value = "syncBidTypeOpportunityToXtDataJobHandler")
@Service
public class SyncBidTypeOpportunityToXtDataJobHandler extends AbstractSyncYcOpportunityDataJobHandler /*implements InitializingBean*/ {
    private String END_TIME        = "endTime";
    private String OPEN_RANGE_TYPE = "openRangeType";
    // 撤项
    private int    WITH_DRAWED     = 12;

    @Override
    public ReturnT<String> execute(String... strings) throws Exception {
        SyncTimeUtil.setCurrentDate();
        logger.info("同步招标项目的商机开始");
        syncOpportunityData();
        logger.info("同步招标项目的商机结束");
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
                        .must(QueryBuilders.termQuery("projectType", BIDDING_PROJECT_TYPE))
                        .must(QueryBuilders.termQuery(BusinessConstant.PLATFORM_SOURCE_KEY, BusinessConstant.YUECAI_SOURCE)));
        Timestamp lastSyncStartTime = new Timestamp(new DateTime(new DateTime().getYear(), 1, 1, 0, 0, 0).getMillis());
        if (Objects.equals(SyncTimeUtil.GMT_TIME, lastSyncTime)) {
            lastSyncTime = lastSyncStartTime;
        }
        logger.info("招标项目商机同步时间lastSyncTime:" + new DateTime(lastSyncTime).toString("yyyy-MM-dd HH:mm:ss") + "\n,"
                + "syncTime:" + SyncTimeUtil.toDateString(SyncTimeUtil.getCurrentDate()));
        syncBiddingProjectDataService(lastSyncTime);
    }

    /**
     * 同步招标项目
     *
     * @param lastSyncTime
     */
    private void syncBiddingProjectDataService(Timestamp lastSyncTime) {
        syncNothingBiddingProjectDataService(lastSyncTime);
        syncPreQualifyBiddingProjectDataService(lastSyncTime);
//        syncTwoStageBiddingProjectDataService(lastSyncTime);
    }

    private void syncNothingBiddingProjectDataService(Timestamp lastSyncTime) {
        logger.info("同步什么都不需要招标项目的商机开始");
        String countNothingSql = "SELECT\n"
                + "   count(1)\n"
                + "FROM\n"
                + "   proj_inter_project pip\n"
                + "JOIN notice_bid nb ON pip.id = nb.PROJECT_ID\n"
                + "WHERE\n"
                + "   pip.TENDER_MODE = 1\n"
                + "AND pip.IS_PREQUALIFY = 2\n"
                + "AND pip.IS_TWO_STAGE = 2\n"
                + "AND nb.BID_ENDTIME IS NOT NULL";
        // 什么都不需要
        String queryNothingSql = "SELECT\n"
                + "    project.ID AS projectId,\n"
                + "    project.PROJECT_NUMBER AS projectCode,\n"
                + "    project.PROJECT_NAME AS projectName,\n"
//                                 + "    project.REGION AS areaStr,\n"
                + "    project.COMPANY_ID AS purchaseId,\n"
                + "    project.PROJECT_STATUS AS projectStatus,\n"
                + "    project.TENDER_NAMES AS purchaseName,\n"
                + "    project.CREATE_TIME AS createTime,\n"
                + "    project.BID_ENDTIME AS endTime,\n"
                + "    product.id AS directoryId,\n"
                + "    product.PRODUCT_NAME AS directoryName\n"
                + "FROM\n"
                + "   (\n"
                + "      SELECT\n"
                + "         pip.ID,\n"
                + "         pip.PROJECT_NUMBER,\n"
                + "         pip.PROJECT_NAME,\n"
                + "         pip.REGION,\n"
                + "         pip.COMPANY_ID,\n"
                + "         pip.PROJECT_STATUS,\n"
                + "         pip.TENDER_NAMES,\n"
                + "         nb.BID_ENDTIME,\n"
                + "         nb.CREATE_TIME\n"
                + "      FROM\n"
                + "         proj_inter_project pip\n"
                + "      JOIN notice_bid nb ON pip.id = nb.PROJECT_ID\n"
                + "      WHERE\n"
                + "         pip.TENDER_MODE = 1\n"
                + "      AND pip.IS_PREQUALIFY = 2\n"
                + "      AND pip.IS_TWO_STAGE = 2\n"
                + "      AND nb.BID_ENDTIME IS NOT NULL"
                + "      LIMIT ?,?\n"
                + "   ) project\n"
                + "LEFT JOIN proj_procurement_product product ON project.ID = product.PROJECT_ID";
        doSyncProjectDataService(ycDataSource, countNothingSql, queryNothingSql, Collections.emptyList());
        logger.info("同步什么都不需要招标项目的商机结束");
    }

    private void syncPreQualifyBiddingProjectDataService(Timestamp lastSyncTime) {
        logger.info("同步资格预审招标项目的商机开始");
        String countPreQualifySql = "SELECT\n"
                + "    count(1)\n"
                + "      FROM\n"
                + "proj_inter_project pip\n"
                + "JOIN pqt_prequalify pp ON pip.id = pp.PROJECT_ID\n"
                + "WHERE\n"
                + "   pip.TENDER_MODE = 1\n"
                + "AND pip.IS_PREQUALIFY = 1\n"
                + "AND pip.IS_TWO_STAGE = 2";
        // 资格预审
        String queryPreQualifySql = "SELECT\n"
                + "    project.ID AS projectId,\n"
                + "    project.PROJECT_NUMBER AS projectCode,\n"
                + "    project.PROJECT_NAME AS projectName,\n"
//                                    + "    project.REGION AS areaStr,\n"
                + "    project.COMPANY_ID AS purchaseId,\n"
                + "    project.PROJECT_STATUS AS projectStatus,\n"
                + "    project.TENDER_NAMES AS purchaseName,\n"
                + "    project.CREATE_TIME AS createTime,\n"
                + "    project.END_TIME AS endTime,\n"
                + "    product.id AS directoryId,\n"
                + "    product.PRODUCT_NAME AS directoryName\n"
                + "FROM\n"
                + "   (\n"
                + "      SELECT\n"
                + "         pip.ID,\n"
                + "         pip.PROJECT_NUMBER,\n"
                + "         pip.PROJECT_NAME,\n"
                + "         pip.REGION,\n"
                + "         pip.COMPANY_ID,\n"
                + "         pip.PROJECT_STATUS,\n"
                + "         pip.TENDER_NAMES,\n"
                + "         pp.CREATE_TIME,\n"
                + "         pp.END_TIME\n"
                + "      FROM\n"
                + "proj_inter_project pip\n"
                + "JOIN pqt_prequalify pp ON pip.id = pp.PROJECT_ID\n"
                + "WHERE\n"
                + "   pip.TENDER_MODE = 1\n"
                + "AND pip.IS_PREQUALIFY = 1\n"
                + "AND pip.IS_TWO_STAGE = 2\n"
                + "LIMIT ?,?\n"
                + ") project\n"
                + "LEFT JOIN proj_procurement_product product ON project.ID = product.PROJECT_ID";
        doSyncProjectDataService(ycDataSource, countPreQualifySql, queryPreQualifySql, Collections.emptyList());
        logger.info("同步资格预审招标项目的商机结束");
    }

    private void syncTwoStageBiddingProjectDataService(Timestamp lastSyncTime) {
        logger.info("同步两阶段招标项目的商机开始");
        String countTwoStageSql = "SELECT\n"
                + "   count(1)\n"
                + "FROM\n"
                + "   proj_inter_project pip\n"
                + "JOIN notice_bid nb ON pip.id = nb.PROJECT_ID\n"
                + "WHERE\n"
                + "   pip.TENDER_MODE = 1\n"
                + "AND pip.IS_PREQUALIFY = 2\n"
                + "AND pip.IS_TWO_STAGE = 1";
        // 两阶段
        String queryTwoStageSql = "SELECT\n"
                + "    project.ID AS projectId,\n"
                + "    project.PROJECT_NUMBER AS projectCode,\n"
                + "    project.PROJECT_NAME AS projectName,\n"
//                                  + "    project.REGION AS areaStr,\n"
                + "    project.COMPANY_ID AS purchaseId,\n"
                + "    project.TENDER_NAMES AS purchaseName,\n"
                + "    project.CREATE_TIME AS createTime,\n"
                + "    project.TECHNICAL_ADVICE_CUT_TIME AS endTime,\n"
                + "    product.id AS directoryId,\n"
                + "    product.PRODUCT_NAME AS directoryName\n"
                + "FROM\n"
                + "   (\n"
                + "      SELECT\n"
                + "         pip.ID,\n"
                + "         pip.PROJECT_NUMBER,\n"
                + "         pip.PROJECT_NAME,\n"
                + "         pip.REGION,\n"
                + "         pip.COMPANY_ID,\n"
                + "         pip.TENDER_NAMES,\n"
                + "         nb.CREATE_TIME,\n"
                + "         nb.TECHNICAL_ADVICE_CUT_TIME\n"
                + "      FROM\n"
                + "         proj_inter_project pip\n"
                + "      JOIN notice_bid nb ON pip.id = nb.PROJECT_ID\n"
                + "      WHERE\n"
                + "         pip.TENDER_MODE = 1\n"
                + "      AND pip.IS_PREQUALIFY = 2\n"
                + "      AND pip.IS_TWO_STAGE = 1\n"
                + "      LIMIT ?,?\n"
                + "   ) project\n"
                + "JOIN proj_procurement_product product ON project.ID = product.PROJECT_ID";
        doSyncProjectDataService(ycDataSource, countTwoStageSql, queryTwoStageSql, Collections.emptyList());
        logger.info("同步两阶段招标项目的商机结束");
    }


    @Override
    protected void refresh(Map<String, Object> result, Map<Long, Set<DirectoryEntity>> projectDirectoryMap) {
        super.refresh(result, projectDirectoryMap);
        // 移除不需要的属性
        result.put(QUOTE_STOP_TIME, SyncTimeUtil.toDateString(result.get(END_TIME)));
        result.remove(END_TIME);
        // 项目类型
        result.put(PROJECT_TYPE, BIDDING_PROJECT_TYPE);
        // 公开类型，默认为1
        result.put(OPEN_RANGE_TYPE, 1);
        // 老平台
        result.put(SOURCE, SOURCE_OLD);
        // 招标撤项项目 商机不展示
        if (Objects.equals(result.get(PROJECT_STATUS), WITH_DRAWED)) {
            result.put(IS_SHOW, HIDDEN);
        } else {
            result.put(IS_SHOW, SHOW);
        }
        // 悦采平台
        result.put(BusinessConstant.PLATFORM_SOURCE_KEY, BusinessConstant.YUECAI_SOURCE);
    }

    @Override
    protected void parseOpportunity(Timestamp currentDate, List<Map<String, Object>> resultToExecute, Map<String, Object> result) {
        Integer projectStatus = (Integer) result.get(PROJECT_STATUS);
        Timestamp endTime = (Timestamp) result.get(END_TIME);
        // 撤销，或者大于招标截止时间，判断为无效商机
        if (projectStatus == WITH_DRAWED || currentDate.after(endTime)) {
            result.put(STATUS, INVALID_OPPORTUNITY_STATUS);
            resultToExecute.add(appendIdToResult(result));
        } else {
            result.put(STATUS, VALID_OPPORTUNITY_STATUS);
            resultToExecute.add(appendIdToResult(result));
        }
    }

//    @Override
//    public void afterPropertiesSet() throws Exception {
//        execute();
//    }
}
