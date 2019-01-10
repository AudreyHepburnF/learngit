package cn.bidlink.job.ycsearch.handler;

import cn.bidlink.job.common.constant.BusinessConstant;
import cn.bidlink.job.common.utils.DBUtil;
import cn.bidlink.job.common.utils.ElasticClientUtil;
import cn.bidlink.job.common.utils.SyncTimeUtil;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.annotation.JobHander;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
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
@Service
@JobHander(value = "syncRecruitOpportunityXtDataJobHandler")
public class SyncRecruitOpportunityXtDataJobHandler extends AbstractSyncYcOpportunityDataJobHandler /*implements InitializingBean*/ {

    private Logger logger = LoggerFactory.getLogger(SyncRecruitOpportunityXtDataJobHandler.class);

    @Autowired
    @Qualifier(value = "recruitDataSource")
    private DataSource recruitDataSource;

    // 是否无期限：1有期限；2无
    private String ENDLESS      = "endless";
    private int    LIMIT        = 1;
    private int    UN_LIMIT     = 2;
    private String INDUSTRY_STR = "industryStr";
    private String COMPTYPE_STR = "compTypeStr";

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
        logger.info("1.1 同步招募信息lastSyncTime:" + SyncTimeUtil.toDateString(lastSyncTime) + "\n" + ",syncTime:" + SyncTimeUtil.currentDateToString());
        this.syncRecruitDataService(lastSyncTime);
        // 修复有限期招募商机的数据
        this.fixedLimitRecruitTypeDataService();
    }

    private void fixedLimitRecruitTypeDataService() {
        logger.info("1.开始修复招募商机数据截止时间到后商机状态");
        Properties properties = elasticClient.getProperties();
        SearchResponse response = elasticClient.getTransportClient().prepareSearch(properties.getProperty("cluster.index"))
                .setTypes(properties.getProperty("cluster.type.supplier_opportunity"))
                .setQuery(QueryBuilders.boolQuery()
                        .must(QueryBuilders.termQuery(BusinessConstant.PLATFORM_SOURCE_KEY, BusinessConstant.YUECAI_SOURCE))
                        .must(QueryBuilders.termQuery(PROJECT_TYPE, RECRUIT_PROJECT_TYPE))
                        .must(QueryBuilders.termQuery(ENDLESS, LIMIT))
                        .must(QueryBuilders.rangeQuery(QUOTE_STOP_TIME).lte(SyncTimeUtil.currentDateToString()))
                ).setScroll(new TimeValue(60000))
                .setSize(pageSize)
                .execute().actionGet();
        do {
            SearchHits hits = response.getHits();
            List<String> ids = new ArrayList<>();
            for (SearchHit hit : hits.getHits()) {
                Map<String, Object> source = hit.getSource();
                ids.add(source.get(ID).toString());
            }
            this.doFixedLimitRecruitTypeDataService(ids);

            response = elasticClient.getTransportClient().prepareSearchScroll(response.getScrollId())
                    .setScroll(new TimeValue(60000))
                    .execute().actionGet();
        } while (response.getHits().getHits().length != 0);
    }

    private void doFixedLimitRecruitTypeDataService(List<String> ids) {
        if (!CollectionUtils.isEmpty(ids)) {
            String countSqlTemplate = "SELECT\n" +
                    "\tcount( 1 ) \n" +
                    "FROM\n" +
                    "\t`recruit` \n" +
                    "WHERE\n" +
                    "\t`STATUS` > 1 \n" +
                    "\tAND id in (%s)";

            String querySqlTemplate = "SELECT\n" +
                    "\tid AS id,\n" +
                    "\tid AS projectId,\n" +
                    "\tTITLE AS projectName,\n" +
                    "\tsdate as quoteStartTime,\n" +
                    "\tedate as quoteStopTime,\n" +
                    "\tAPPLY_COUNT AS biddenSupplierCount,\n" +
//                "\tAREA AS areaCode,\n" +
//                "\tAREA_NAME AS areaStr,\n" +
//                "\tOPERATE_MODE_NAME as compTypeStr,\n" +
                    "\tPURCHASER_ID AS purchaseId,\n" +
                    "\tPURCHASER AS purchaseName,\n" +
                    "\tstatus,\n" +
                    "\tendless,\n" +
                    "\tDEL_FLAG AS isShow,\n" +
                    "\tCREATE_TIME AS createTime,\n" +
                    "\tUPDATE_TIME AS updateTime \n" +
                    "FROM\n" +
                    "\t`recruit` \n" +
                    "WHERE\n" +
                    "\t`STATUS` > 1 \n" +
                    "\tAND id in (%s) limit ?,?";
            String countSql = String.format(countSqlTemplate, StringUtils.collectionToDelimitedString(ids, ",", "'", "'"));
            String querySql = String.format(querySqlTemplate, StringUtils.collectionToDelimitedString(ids, ",", "'", "'"));
            doSncRecruitDataService(recruitDataSource, countSql, querySql, Collections.emptyList());
        }
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
                "\tsdate as quoteStartTime,\n" +
                "\tedate as quoteStopTime,\n" +
                "\tAPPLY_COUNT AS biddenSupplierCount,\n" +
//                "\tAREA AS areaCode,\n" +
//                "\tAREA_NAME AS areaStr,\n" +
//                "\tOPERATE_MODE_NAME as compTypeStr,\n" +
                "\tPURCHASER_ID AS purchaseId,\n" +
                "\tPURCHASER AS purchaseName,\n" +
                "\tstatus,\n" +
                "\tendless,\n" +
                "\tDEL_FLAG AS isShow,\n" +
                "\tCREATE_TIME AS createTime,\n" +
                "\tUPDATE_TIME AS updateTime \n" +
                "FROM\n" +
                "\t`recruit` \n" +
                "WHERE\n" +
                "\t`STATUS` > 1 \n" +
                "\tAND CREATE_TIME > ? limit ?,?";
        doSncRecruitDataService(recruitDataSource, insertCountSql, insertQuerySql, Collections.singletonList(lastSyncTime));

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
                "\tsdate as quoteStartTime,\n" +
                "\tedate as quoteStopTime,\n" +
                "\tAPPLY_COUNT AS biddenSupplierCount,\n" +
//                "\tAREA AS areaCode,\n" +
//                "\tAREA_NAME AS areaStr,\n" +
//                "\tOPERATE_MODE_NAME as compTypeStr,\n" +
                "\tPURCHASER_ID AS purchaseId,\n" +
                "\tPURCHASER AS purchaseName,\n" +
                "\tstatus,\n" +
                "\tendless,\n" +
                "\tDEL_FLAG AS isShow,\n" +
                "\tCREATE_TIME AS createTime,\n" +
                "\tUPDATE_TIME AS updateTime \n" +
                "FROM\n" +
                "\t`recruit` \n" +
                "WHERE\n" +
                "\t`STATUS` > 1 \n" +
                "\tAND update_time > ? limit ?,?";
        doSncRecruitDataService(recruitDataSource, updateCountSql, updateQuerySql, Collections.singletonList(lastSyncTime));
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
                    "\tINDUSTRY_STR," +
                    "workpattern\n" +
                    "FROM\n" +
                    "\tt_reg_company \n" +
                    "WHERE\n" +
                    "\tid IN (%s)";
            String querySql = String.format(querySqlTemplate, StringUtils.collectionToCommaDelimitedString(purchaserIds));
            Map<Long, CompanyInfo> companyIndustryMap = DBUtil.query(uniregDataSource, querySql, null, new DBUtil.ResultSetCallback<Map<Long, CompanyInfo>>() {
                @Override
                public Map<Long, CompanyInfo> execute(ResultSet resultSet) throws SQLException {
                    Map<Long, CompanyInfo> map = new HashMap<>();
                    while (resultSet.next()) {
                        map.put(resultSet.getLong(1), new CompanyInfo(resultSet.getString(2), resultSet.getString(3)));
                    }
                    return map;
                }
            });
            for (Map<String, Object> map : mapList) {
                CompanyInfo companyInfo = companyIndustryMap.get(Long.valueOf(map.get(PURCHASE_ID).toString()));
                if (!Objects.isNull(companyInfo)) {
                    map.put(INDUSTRY_STR, companyInfo.getIndustryStr());
                    map.put(COMPTYPE_STR, companyInfo.getCompTypeStr());
                }
            }
        }
    }

    private void handlerData(Map<String, Object> map) {
        Object status = map.get(STATUS);
        // 项目状态
        if (!Objects.isNull(status) && Objects.equals(status, UNDERWAY)) {
            if (Objects.equals(map.get(ENDLESS), LIMIT) && ((Date) map.get(QUOTE_STOP_TIME)).after(new Date())) {
                map.put(STATUS, VALID_OPPORTUNITY_STATUS);
            } else if (Objects.equals(map.get(ENDLESS), UN_LIMIT)) {
                map.put(STATUS, VALID_OPPORTUNITY_STATUS);
            } else {
                map.put(STATUS, INVALID_OPPORTUNITY_STATUS);
            }
        } else {
            map.put(STATUS, INVALID_OPPORTUNITY_STATUS);
        }

        // 截止时间
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
    }

    @Override
    protected void parseOpportunity(Timestamp currentDate, List<Map<String, Object>> resultToExecute, Map<String, Object> result) {

    }

    class CompanyInfo {
        private String industryStr;
        private String compTypeStr; // 经营模式  es字段加错导致名字问题

        public CompanyInfo(String industryStr, String compTypeStr) {
            this.industryStr = industryStr;
            this.compTypeStr = compTypeStr;
        }

        public String getIndustryStr() {
            return industryStr;
        }

        public void setIndustryStr(String industryStr) {
            this.industryStr = industryStr;
        }

        public String getCompTypeStr() {
            return compTypeStr;
        }

        public void setCompTypeStr(String compTypeStr) {
            this.compTypeStr = compTypeStr;
        }
    }

//    @Override
//    public void afterPropertiesSet() throws Exception {
//        execute();
//    }
}
