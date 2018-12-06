package cn.bidlink.job.ycsearch.handler;

import cn.bidlink.job.common.constant.BusinessConstant;
import cn.bidlink.job.common.utils.DBUtil;
import cn.bidlink.job.common.utils.ElasticClientUtil;
import cn.bidlink.job.common.utils.SyncTimeUtil;
import com.xxl.job.core.biz.model.ReturnT;
import org.elasticsearch.index.query.QueryBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author <a href="mailto:zhihuizhou@ebnew.com">wisdom</a>
 * @version Ver 1.0
 * @description:同步招募基本信息到商机表中
 * @Date 2018/12/5
 */
@Service(value = "syncRecruitOpportunityXtDataJobHandler")
public class SyncRecruitOpportunityXtDataJobHandler extends AbstractSyncYcOpportunityDataJobHandler/* implements InitializingBean*/ {

    private Logger logger = LoggerFactory.getLogger(SyncRecruitOpportunityXtDataJobHandler.class);

    @Autowired
    @Qualifier(value = "proDataSource")
    private DataSource proDataSource;

    private String AREA_CODE    = "areaCode";
    private String S_DATE       = "sdate";
    private String E_DATE       = "edate";
    private String INDUSTRY_STR = "industryStr";

    private int UNDERWAY = 2;

    @Override
    public ReturnT<String> execute(String... strings) throws Exception {
        SyncTimeUtil.setCurrentDate();
        logger.info("1.开始同步招募信息数据");
        syncRecruitData();
        logger.info("2.结束同步招募信息数据");
        return ReturnT.SUCCESS;
    }

    private void syncRecruitData() {
        Timestamp lastSyncTime = ElasticClientUtil.getMaxTimestamp(elasticClient, "cluster.index", "cluster.type.supplier_opportunity",
                QueryBuilders.boolQuery().must(QueryBuilders.termQuery(BusinessConstant.PLATFORM_SOURCE_KEY, BusinessConstant.YUECAI_SOURCE))
                        .must(QueryBuilders.termQuery(PROJECT_TYPE, RECRUIT_PROJECT_TYPE)));
//        Timestamp lastSyncTime = new Timestamp(0);
        logger.info("1.1 同步招募信息lastSyncTime:" + SyncTimeUtil.toDateString(lastSyncTime) + "\n" + ",syncTime:" + SyncTimeUtil.currentDateToString());
        syncRecruitDataService(lastSyncTime);
    }

    private void syncRecruitDataService(Timestamp lastSyncTime) {
        String insertCountSql = "SELECT\n" +
                "\tcount( 1 ) \n" +
                "FROM\n" +
                "\t`recruit` \n" +
                "WHERE\n" +
                "\t`STATUS` > 1 \n" +
                "\tAND CREATE_TIME > ?";

        String insertQuerySql = "SELECT\n" +
                "\tid AS id,\n" +
                "\tid AS projectId,\n" +
                "\tTITLE AS projectName,\n" +
                "\tsdate,\n" +
                "\tedate,\n" +
                "\tAPPLY_COUNT AS biddenSupplierCount,\n" +
//                "\tAREA AS areaCode,\n" +
//                "\tAREA_NAME AS areaStr,\n" +
//                "\tOPERATE_MODE_NAME as compTypeStr,\n" +
                "\tPURCHASER_ID AS purchaseId,\n" +
                "\tPURCHASER AS purchaseName,\n" +
                "\tstatus,\n" +
//                "\tendless,\n" +
                "\tDEL_FLAG AS isShow,\n" +
                "\tCREATE_TIME AS createTime,\n" +
                "\tUPDATE_TIME AS updateTime \n" +
                "FROM\n" +
                "\t`recruit` \n" +
                "WHERE\n" +
                "\t`STATUS` > 1 \n" +
                "\tAND CREATE_TIME > ? limit ?,?";
        doSncRecruitDataService(proDataSource, insertCountSql, insertQuerySql, Collections.singletonList(lastSyncTime));

        String updateCountSql = "SELECT\n" +
                "\tcount( 1 ) \n" +
                "FROM\n" +
                "\t`recruit` \n" +
                "WHERE\n" +
                "\t`STATUS` > 1 \n" +
                "\tAND update_time > ?";

        String updateQuerySql = "SELECT\n" +
                "\tid AS id,\n" +
                "\tid AS projectId,\n" +
                "\tTITLE AS projectName,\n" +
                "\tsdate,\n" +
                "\tedate,\n" +
                "\tAPPLY_COUNT AS biddenSupplierCount,\n" +
//                "\tAREA AS areaCode,\n" +
//                "\tAREA_NAME AS areaStr,\n" +
//                "\tOPERATE_MODE_NAME as compTypeStr,\n" +
                "\tPURCHASER_ID AS purchaseId,\n" +
                "\tPURCHASER AS purchaseName,\n" +
                "\tstatus,\n" +
//                "\tendless,\n" +
                "\tDEL_FLAG AS isShow,\n" +
                "\tCREATE_TIME AS createTime,\n" +
                "\tUPDATE_TIME AS updateTime \n" +
                "FROM\n" +
                "\t`recruit` \n" +
                "WHERE\n" +
                "\t`STATUS` > 1 \n" +
                "\tAND update_time > ? limit ?,?";
        doSncRecruitDataService(proDataSource, updateCountSql, updateQuerySql, Collections.singletonList(lastSyncTime));
    }

    private void doSncRecruitDataService(DataSource dataSource, String countSql, String querySql, List<Object> params) {
        long count = DBUtil.count(dataSource, countSql, params);
        logger.debug("执行countSql : {}, params : {}，共{}条", countSql, params, count);
        if (count > 0) {
            for (long i = 0; i < count; ) {
                List<Object> paramsToUse = appendToParams(params, i);
                // 查出符合条件的商机
                List<Map<String, Object>> mapList = DBUtil.query(dataSource, querySql, paramsToUse);
                logger.debug("执行querySql : {}, params : {}，共{}条", querySql, paramsToUse, mapList.size());
                for (Map<String, Object> map : mapList) {
                    handlerData(map);
                }
                // 处理采购商id
                Set<Long> purchaseIds = new HashSet<>();
                for (Map<String, Object> result : mapList) {
                    purchaseIds.add(Long.valueOf(result.get(PURCHASE_ID).toString()));
                }

                // 添加区域
                appendAreaStrToResult(mapList, purchaseIds);
                this.appendIndustry(mapList);
                // 处理商机的状态
                batchExecute(mapList);
                i += pageSize;
            }
        }

    }

    private void appendIndustry(List<Map<String, Object>> mapList) {
        if (!CollectionUtils.isEmpty(mapList)) {
            Set<Long> purchaserIds = mapList.stream().map(map -> Long.valueOf(map.get(PURCHASE_ID).toString())).collect(Collectors.toSet());
            String querySqlTemplate = "SELECT\n" +
                    "\tid ,\n" +
                    "\tINDUSTRY_STR\n" +
                    "FROM\n" +
                    "\tt_reg_company \n" +
                    "WHERE\n" +
                    "\tid IN (%s)";
            String querySql = String.format(querySqlTemplate, StringUtils.collectionToCommaDelimitedString(purchaserIds));
            Map<Long, Object> companyIndustryMap = DBUtil.query(uniregDataSource, querySql, null, new DBUtil.ResultSetCallback<Map<Long, Object>>() {
                @Override
                public Map<Long, Object> execute(ResultSet resultSet) throws SQLException {
                    Map<Long, Object> map = new HashMap<>();
                    while (resultSet.next()) {
                        map.put(resultSet.getLong(1), resultSet.getString(2));
                    }
                    return map;
                }
            });
            for (Map<String, Object> map : mapList) {
                map.put(INDUSTRY_STR, companyIndustryMap.get(Long.valueOf(map.get(PURCHASE_ID).toString())));
            }
        }
    }

    private void handlerData(Map<String, Object> map) {

        Object sDate = map.get(S_DATE);
        Object eDate = map.get(E_DATE);
        if (!Objects.isNull(sDate) && !Objects.isNull(eDate)) {
            map.put(QUOTE_STOP_TIME, eDate);
            map.put(QUOTE_STOP_TIME_STR, SyncTimeUtil.toDateString(sDate) + " 至 " + SyncTimeUtil.toDateString(eDate));
        }
        Object status = map.get(STATUS);
        if (!Objects.isNull(status) && Objects.equals(status, UNDERWAY)) {
            map.put(STATUS, VALID_OPPORTUNITY_STATUS);
        } else {
            map.put(STATUS, INVALID_OPPORTUNITY_STATUS);
        }
        Object isShow = map.get(IS_SHOW);
        // 删除不展示
        if (!Objects.isNull(isShow) && Objects.equals(isShow, 1)) {
            map.put(IS_SHOW, SHOW);
        } else {
            map.put(IS_SHOW, HIDDEN);
        }
        map.put(PROJECT_NAME_NOT_ANALYZED, map.get(PROJECT_NAME));
        map.put(PURCHASE_NAME_NOT_ANALYZED, map.get(PURCHASE_NAME));
        map.put(PROJECT_TYPE, RECRUIT_PROJECT_TYPE);
        map.put(PURCHASE_ID, String.valueOf(map.get(PURCHASE_ID)));
        map.put(BusinessConstant.PLATFORM_SOURCE_KEY, BusinessConstant.YUECAI_SOURCE);
        map.put(SyncTimeUtil.SYNC_TIME, SyncTimeUtil.getCurrentDate());
        map.remove(S_DATE);
        map.remove(E_DATE);
    }

    @Override
    protected void parseOpportunity(Timestamp currentDate, List<Map<String, Object>> resultToExecute, Map<String, Object> result) {

    }

//    @Override
//    public void afterPropertiesSet() throws Exception {
//        execute();
//    }
}
