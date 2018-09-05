package cn.bidlink.job.business.handler;

import cn.bidlink.job.common.constant.BusinessConstant;
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



/**
 * 同步协同平台采购商机数据
 *
 * @author : <a href="mailto:zikaifeng@ebnew.com">冯子恺</a>
 * @version : Ver 1.0
 * @description :
 * @date : 2017/8/7
 */
@JobHander(value = "syncPurchaseTypeOpportunityDataJobHandler")
@Service
public class SyncPurchaseTypeOpportunityDataJobHandler extends AbstractSyncOpportunityDataJobHandler /*implements InitializingBean*/ {

    
    private String REAL_QUOTE_STOP_TIME = "realQuoteStopTime";
    private String PROVINCE             = "province";
    private String IS_CORE              = "isCore";

    @Override
    public ReturnT<String> execute(String... strings) throws Exception {
        SyncTimeUtil.setCurrentDate();
        logger.info("同步协同采购项目的商机开始");
        syncOpportunityData();
        logger.info("同步协同采购项目的商机结束");
        return ReturnT.SUCCESS;
    }

    /**
     * 同步采购商机
     */
    private void syncOpportunityData() {
        Timestamp lastSyncTime = ElasticClientUtil.getMaxTimestamp(elasticClient,
                                                                   "cluster.index",
                                                                   "cluster.type.supplier_opportunity",
                                                                   QueryBuilders.boolQuery()
                                                                           .must(QueryBuilders.termQuery("projectType", PURCHASE_PROJECT_TYPE))
                                                                           .must(QueryBuilders.termQuery("source", SOURCE_NEW)));
        logger.info("协同采购项目商机同步时间：" + new DateTime(lastSyncTime).toString("yyyy-MM-dd HH:mm:ss"));
//        Timestamp lastSyncTime = SyncTimeUtil.GMT_TIME;
        syncPurchaseProjectDataService(lastSyncTime);
    }

    /**
     * 同步采购项目
     *
     * @param lastSyncTime
     */
    private void syncPurchaseProjectDataService(Timestamp lastSyncTime) {
        String countUpdatedSql = "SELECT\n"
                                 + "   count(1)\n"
                                 + "FROM\n"
                                 + "   purchase_project pp\n"
                                 + "LEFT JOIN purchase_project_control ppc ON pp.id = ppc.id\n"
                                 + "AND pp.company_id = ppc.company_id\n"
                                 + "WHERE\n"
                                 + "   pp.process_status > 13\n"
                                 + "AND ppc.project_open_range = 1 AND pp.update_time > ?";
        String queryUpdatedSql = "SELECT\n"
                                 + "   s.*, ppi.id AS directoryId,ppi.`name` AS directoryName\n"
                                 + "FROM\n"
                                 + "   (\n"
                                 + "      SELECT\n"
                                 + "         pp.company_id AS purchaseId,\n"
                                 + "         pp.company_name AS purchaseName,\n"
                                 + "         pp.id AS projectId,\n"
                                 + "         pp. code AS projectCode,\n"
                                 + "         pp.`name` AS projectName,\n"
                                 + "         ppc.project_open_range AS openRangeType,\n"
                                 + "         ppc.is_core AS isCore,\n"
                                 + "         pp.project_status AS projectStatus,\n"
                                 + "         pp.process_status AS processStatus,\n"
                                 + "         pp.quote_stop_time AS quoteStopTime,\n"
                                 + "         pp.real_quote_stop_time AS realQuoteStopTime,\n"
                                 + "         pp.zone_str AS areaStr,\n"
                                 + "         pp.province AS province,\n"
                                 + "         pp.link_man AS linkMan,\n"
                                 + "         pp.sys_id AS sourceId,\n"
                                 + "         CASE\n"
                                 + "      WHEN ppc.is_show_mobile = 1 THEN\n"
                                 + "         pp.link_phone\n"
                                 + "      WHEN ppc.is_show_tel = 1 THEN\n"
                                 + "         pp.link_tel\n"
                                 + "      ELSE\n"
                                 + "         NULL\n"
                                 + "      END AS linkPhone,\n"
                                 + "      pp.create_time AS createTime,\n"
                                 + "      pp.update_time AS updateTime\n"
                                 + "   FROM\n"
                                 + "      purchase_project pp\n"
                                 + "   LEFT JOIN purchase_project_control ppc ON pp.id = ppc.id\n"
                                 + "   AND pp.company_id = ppc.company_id\n"
                                 + "   WHERE\n"
                                 + "   pp.process_status > 13\n"
                                 + "   AND ppc.project_open_range = 1 AND pp.update_time > ?\n"
                                 + "   LIMIT ?,?\n"
                                 + "   ) s\n"
                                 + "LEFT JOIN purchase_project_item ppi ON s.projectId = ppi.project_id\n"
                                 + "AND s.purchaseId = ppi.company_id";
        doSyncProjectDataService(purchaseDataSource, countUpdatedSql, queryUpdatedSql, Collections.singletonList((Object) lastSyncTime));
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

    /**
     * 解析商机数据
     *
     * @param currentDate     当前同步的时间
     * @param resultToExecute 需要更新的数据
     * @param result          从数据库获取的数据
     */
    @Override
    protected void parseOpportunity(Timestamp currentDate, List<Map<String, Object>> resultToExecute, Map<String, Object> result) {
        int PROJECT_EXECUTING = 1;  // 项目正在进行
        int PROCESS_TO_QUOTE = 20;  // 待截标
        int processStatus = (int) result.get(PROCESS_STATUS);
        int projectStatus = (int) result.get(PROJECT_STATUS);
        Timestamp quoteStopTime = (Timestamp) result.get(QUOTE_STOP_TIME);
        // 小于截止时间且待截标且进行中，则为商机，否则不是商机
        if (currentDate.before(quoteStopTime)
            && processStatus == PROCESS_TO_QUOTE
            && projectStatus == PROJECT_EXECUTING) {
            result.put(STATUS, VALID_OPPORTUNITY_STATUS);
            resultToExecute.add(appendIdToResult(result));
        } else {
            result.put(STATUS, INVALID_OPPORTUNITY_STATUS);
            resultToExecute.add(appendIdToResult(result));
        }
    }

    @Override
    protected void refresh(Map<String, Object> result, Map<Long, Set<DirectoryEntity>> projectDirectoryMap) {
        super.refresh(result, projectDirectoryMap);
        // 转换字段类型
        Object isCore = result.get(IS_CORE);
        if (isCore instanceof Boolean) {
            result.put("isCore", isCore != null && ((Boolean) isCore) ? 1 : 0);
        }
        result.put(QUOTE_STOP_TIME, SyncTimeUtil.toDateString(result.get(QUOTE_STOP_TIME)));
        // 移除不需要的属性
        result.remove(REAL_QUOTE_STOP_TIME);
        result.remove(PROVINCE);
        // 添加不分词的areaStr
        result.put(AREA_STR_NOT_ANALYZED, result.get(AREA_STR));
        // 项目类型
        result.put(PROJECT_TYPE, PURCHASE_PROJECT_TYPE);
        // 新平台
        result.put(SOURCE, SOURCE_NEW);

        //添加平台来源
        result.put(BusinessConstant.PLATFORM_SOURCE_KEY,BusinessConstant.IXIETONG_SOURCE);
    }


//    @Override
//    public void afterPropertiesSet() throws Exception {
//        execute();
//    }
}
