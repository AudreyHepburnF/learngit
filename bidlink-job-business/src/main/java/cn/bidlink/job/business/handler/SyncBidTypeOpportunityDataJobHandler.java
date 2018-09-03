package cn.bidlink.job.business.handler;

import cn.bidlink.job.business.constant.BusinessConstant;
import cn.bidlink.job.business.utils.AreaUtil;
import cn.bidlink.job.common.utils.ElasticClientUtil;
import cn.bidlink.job.common.utils.SyncTimeUtil;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.annotation.JobHander;
import org.elasticsearch.index.query.QueryBuilders;
import org.joda.time.DateTime;
import org.springframework.stereotype.Service;
import org.springframework.util.DigestUtils;
import org.springframework.util.StringUtils;

import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static cn.bidlink.job.business.utils.AreaUtil.queryAreaInfo;


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
public class SyncBidTypeOpportunityDataJobHandler extends AbstractSyncOpportunityDataJobHandler /*implements InitializingBean */{

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
                        .must(QueryBuilders.termQuery("projectType", BIDDING_PROJECT_TYPE))
                        .must(QueryBuilders.termQuery("source", SOURCE_NEW)));
        logger.info("招标项目商机同步时间：" + new DateTime(lastSyncTime).toString("yyyy-MM-dd HH:mm:ss"));
//        Timestamp lastSyncTime = SyncTimeUtil.GMT_TIME;
        syncBiddingProjectDataService(lastSyncTime);
    }

    /**
     * 同步招标项目
     *
     * @param lastSyncTime
     */
    private void syncBiddingProjectDataService(Timestamp lastSyncTime) {
        String countUpdatedSql = "SELECT\n"
                + "   count(1)\n"
                + "FROM\n"
                + "   bid_sub_project\n"
                + "WHERE\n"
                + "   is_bid_open = 1 AND node > 1 AND approve_status = 2 AND update_time > ?";
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
                    "\tis_bid_open = 1 \n" +
                    "\tAND node > 1 \n" +
                    "\tAND bsp.approve_status = 2\n" +
                    "\tAND bsp.update_time > ? \n" +
                    "\tLIMIT ?,? \n" +
                "\t) project\n" +
                "\tLEFT JOIN bid_project_item bpi ON project.projectId = bpi.sub_project_id";
        doSyncProjectDataService(tenderDataSource, countUpdatedSql, queryUpdatedSql, Collections.singletonList(((Object) lastSyncTime)));
    }


    @Override
    protected void appendTenantKeyAndAreaStrToResult(List<Map<String, Object>> resultToExecute, Set<Long> purchaseIds) {
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

    @Override
    protected String generateOpportunityId(Map<String, Object> result) {
        Long projectId = (Long) result.get(PROJECT_ID);
        Long purchaseId = (Long) result.get(PURCHASE_ID);
        if (projectId == null) {
            throw new RuntimeException("商机ID生成失败，原因：项目ID为空!");
        }
        if (StringUtils.isEmpty(purchaseId)) {
            throw new RuntimeException("商机ID生成失败，原因：采购商ID为空!");
        }

        return DigestUtils.md5DigestAsHex((projectId + "_" + purchaseId + "_" + SOURCE_NEW).getBytes());
    }

    @Override
    protected void refresh(Map<String, Object> result, Map<Long, Set<DirectoryEntity>> projectDirectoryMap) {
        super.refresh(result, projectDirectoryMap);
        result.put(QUOTE_STOP_TIME, SyncTimeUtil.toDateString(result.get(QUOTE_STOP_TIME)));
        // 项目类型
        result.put(PROJECT_TYPE, BIDDING_PROJECT_TYPE);
        // 公开类型，默认为1
        result.put(OPEN_RANGE_TYPE, 1);
        // 新平台
        result.put(SOURCE, SOURCE_NEW);
        //添加平台来源
        result.put(BusinessConstant.PLATFORM_SOURCE_KEY,BusinessConstant.IXIETONG_SOURCE);
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
            resultToExecute.add(appendIdToResult(result));
        } else {
            result.put(STATUS, INVALID_OPPORTUNITY_STATUS);
            resultToExecute.add(appendIdToResult(result));
        }
    }


//    @Override
//    public void afterPropertiesSet() throws Exception {
//        execute();
//    }
}
