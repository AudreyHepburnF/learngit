package cn.bidlink.job.business.handler;

import cn.bidlink.job.business.utils.RegionUtil;
import cn.bidlink.job.common.utils.ElasticClientUtil;
import cn.bidlink.job.common.utils.SyncTimeUtil;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.annotation.JobHander;
import org.elasticsearch.index.query.QueryBuilders;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.util.DigestUtils;
import org.springframework.util.StringUtils;

import javax.sql.DataSource;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * 同步协同平台采购商机数据
 * 注意：可以代替{@link SyncPurchaseTypeOpportunityDataJobHandler}
 *
 * @author : <a href="mailto:zikaifeng@ebnew.com">冯子恺</a>
 * @version : Ver 1.0
 * @description :
 * @date : 2017/8/7
 */
@JobHander(value = "syncXieTongPurchaseTypeOpportunityDataJobHandler")
@Service
public class SyncXieTongPurchaseTypeOpportunityDataJobHandler extends AbstractSyncOpportunityDataJobHandler /*implements InitializingBean*/ {
    @Autowired
    @Qualifier("synergyDataSource")
    protected DataSource synergyDataSource;

    private String REAL_QUOTE_STOP_TIME = "realQuoteStopTime";
    private String PROVINCE             = "province";

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
                                                                           .must(QueryBuilders.termQuery("source", SOURCE_NEW)));
        logger.info("采购项目商机同步时间：" + new DateTime(lastSyncTime).toString("yyyy-MM-dd HH:mm:ss"));
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
                                 + "   pp.project_status = 2\n"
                                 + "AND ppc.project_open_range = 1 AND pp.update_time > ?";
        String queryUpdatedSql = "SELECT\n"
                                 + "   s.*, ppi.`name` AS directoryName\n"
                                 + "FROM\n"
                                 + "   (\n"
                                 + "      SELECT\n"
                                 + "         pp.province,\n"
                                 + "         pp.company_id AS purchaseId,\n"
                                 + "         pp.company_name AS purchaseName,\n"
                                 + "         pp.id AS projectId,\n"
                                 + "         pp. code AS projectCode,\n"
                                 + "         pp.`name` AS projectName,\n"
                                 + "         ppc.project_open_range AS openRangeType,\n"
                                 + "         ppc.is_core AS isCore,\n"
                                 + "         pp.project_status AS projectStatus,\n"
                                 + "         pp.quote_stop_time AS quoteStopTime,\n"
                                 + "         pp.real_quote_stop_time AS realQuoteStopTime,\n"
                                 + "         pp.zone_str AS areaStr,\n"
                                 + "         pp.link_man AS linkMan,\n"
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
                                 + "      pp.project_status = 2\n"
                                 + "   AND ppc.project_open_range = 1 AND pp.update_time > ?\n"
                                 + "   LIMIT ?,?\n"
                                 + "   ) s\n"
                                 + "LEFT JOIN purchase_project_item ppi ON s.projectId = ppi.project_id\n"
                                 + "AND s.purchaseId = ppi.company_id";
        doSyncProjectDataService(synergyDataSource, countUpdatedSql, queryUpdatedSql, Collections.singletonList((Object) lastSyncTime));
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

    protected void appendTenantKeyAndAreaStrToResult(List<Map<String, Object>> resultToExecute, Set<Long> purchaseIds) {
        if (purchaseIds.size() > 0) {
            for (Map<String, Object> result : resultToExecute) {
                Object value = result.get(PROVINCE);
                if (value != null) {
                    String regionKey = ((String) value).substring(0, 2);
                    result.put(REGION, RegionUtil.regionMap.get(regionKey));
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
    protected void parseOpportunity(Timestamp currentDate, List<Map<String, Object>> resultToExecute, Map<String, Object> result) {
        // 判断商机
        Timestamp quoteStopTime = (Timestamp) result.get(QUOTE_STOP_TIME);
        Timestamp realQuoteStopTime = (Timestamp) result.get(REAL_QUOTE_STOP_TIME);
        // 已截标
        if (realQuoteStopTime != null) {
            result.put(STATUS, INVALID_OPPORTUNITY_STATUS);
            resultToExecute.add(appendIdToResult(result));
        } else {
            // 未过期
            if (quoteStopTime.before(currentDate)) {
                result.put(STATUS, VALID_OPPORTUNITY_STATUS);
                resultToExecute.add(appendIdToResult(result));
            } else {
                result.put(STATUS, INVALID_OPPORTUNITY_STATUS);
                resultToExecute.add(appendIdToResult(result));
            }
        }
    }

    protected void refresh(Map<String, Object> result, Map<Long, Set<String>> projectDirectoryMap) {
        super.refresh(result, projectDirectoryMap);
        // 移除不需要的属性
        result.put(QUOTE_STOP_TIME, SyncTimeUtil.toDateString(result.get(QUOTE_STOP_TIME)));
        result.remove(REAL_QUOTE_STOP_TIME);
        result.remove(PROVINCE);
        // 添加不分词的areaStr
        result.put(AREA_STR_NOT_ANALYZED, result.get(AREA_STR));
        // 项目类型
        result.put(PROJECT_TYPE, PURCHASE_PROJECT_TYPE);
        // 新平台
        result.put(SOURCE, SOURCE_NEW);
    }


//    @Override
//    public void afterPropertiesSet() throws Exception {
//        execute();
//    }
}
