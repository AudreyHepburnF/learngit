package cn.bidlink.job.business.handler;

import cn.bidlink.job.common.es.ElasticClient;
import cn.bidlink.job.common.utils.DBUtil;
import cn.bidlink.job.common.utils.ElasticClientUtil;
import cn.bidlink.job.common.utils.SyncTimeUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.ValueFilter;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.annotation.JobHander;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.MessageFormat;
import java.util.*;


/**
 * @author : <a href="mailto:zikaifeng@ebnew.com">冯子恺</a>
 * @version : Ver 1.0
 * @description :
 * @date : 2017/11/27
 */
@JobHander(value = "syncSupplierDataJobHandler")
@Service
public class SyncSupplierDataJobHandler extends JobHandler implements InitializingBean {
    private Logger logger = LoggerFactory.getLogger(SyncSupplierDataJobHandler.class);

    @Autowired
    private ElasticClient elasticClient;

    @Autowired
    @Qualifier("creditDataSource")
    private DataSource creditDataSource;

    @Autowired
    @Qualifier("centerDataSource")
    private DataSource centerDataSource;

    @Value("${enterpriseSpaceFormat}")
    private String enterpriseSpaceFormat;

    private String ID               = "id";
    private String CORE             = "core";
    private String AREA_STR         = "areaStr";
    private String ZONE_STR         = "zoneStr";
    private String AREA             = "area";
    private String CITY             = "city";
    private String COUNTY           = "county";
    private String CREDIT_RATING    = "creditRating";
    private String AUTH_CODE_ID     = "authCodeId";
    private String AUTHEN_NUMBER    = "authenNumber";
    private String CODE             = "code";
    private String FUND             = "fund";
    private String TENANT_ID        = "tenantId";
    private String MOBILE           = "mobile";
    private String TEL              = "tel";
    private String LOGIN_NAME       = "loginName";
    private String ENTERPRISE_SPACE = "enterpriseSpace";

    @Override
    public void afterPropertiesSet() throws Exception {
        pageSize = 1000;
    }

    @Override
    public ReturnT<String> execute(String... strings) throws Exception {
        SyncTimeUtil.setCurrentDate();
        logger.info("供应商数据同步开始");
        syncSupplierData();
        logger.info("供应商数据同步结束");
        return ReturnT.SUCCESS;
    }

    private void syncSupplierData() {
        Timestamp lastSyncTime = ElasticClientUtil.getMaxTimestamp(elasticClient, "cluster.index", "cluster.type.supplier", null);
        logger.info("供应商数据同步时间：" + new DateTime(lastSyncTime).toString("yyyy-MM-dd HH:mm:ss") + "\n"
                    + ", syncTime : " + new DateTime(SyncTimeUtil.getCurrentDate()).toString("yyyy-MM-dd HH:mm:ss"));
        syncSupplierDataService(lastSyncTime);
        syncSupplierCompanyStatusService(lastSyncTime);

    }

    private void syncSupplierCompanyStatusService(Timestamp lastSyncTime) {
        if (SyncTimeUtil.GMT_TIME.equals(lastSyncTime)) {
            logger.info("首次同步，忽略同步供应商状态");
            return;
        }

        logger.info("同步供应商状态开始");
        String countInsertedSql = "SELECT count(1) FROM t_uic_company_status WHERE CREATE_TIME > ?";
        String queryInsertedSql = "SELECT COMP_ID AS id, CREDIT_MEDAL_STATUS AS core FROM t_uic_company_status WHERE CREATE_TIME > ? GROUP BY COMP_ID ORDER BY CREATE_TIME LIMIT ?, ?";
        doSyncSupplierCompanyStatus(countInsertedSql, queryInsertedSql, lastSyncTime);

        String countUpdatedSql = "SELECT count(1) FROM t_uic_company_status WHERE UPDATE_TIME > ?";
        String queryUpdatedSql = "SELECT COMP_ID AS id, CREDIT_MEDAL_STATUS AS core FROM t_uic_company_status WHERE UPDATE_TIME > ? GROUP BY COMP_ID ORDER BY UPDATE_TIME LIMIT ?, ?";
        doSyncSupplierCompanyStatus(countUpdatedSql, queryUpdatedSql, lastSyncTime);
        logger.info("同步供应商状态结束");
    }

    private void doSyncSupplierCompanyStatus(String countSql, String querySql, Timestamp lastSyncTime) {
        List<Object> params = new ArrayList<>();
        params.add(lastSyncTime);
        long count = DBUtil.count(centerDataSource, countSql, params);
        logger.debug("执行countSql : {}, params : {}，共{}条", countSql, params, count);
        if (count > 0) {
            for (long i = 0; i < count; i += pageSize) {
                List<Object> paramsToUse = appendToParams(params, i);
                // 查出符合条件的供应商
                Map<String, Object> supplierCoreStatusMap = DBUtil.query(centerDataSource, querySql, paramsToUse, new DBUtil.ResultSetCallback<Map<String, Object>>() {
                    @Override
                    public Map<String, Object> execute(ResultSet resultSet) throws SQLException {
                        Map<String, Object> map = new HashMap<>();
                        while (resultSet.next()) {
                            map.put(String.valueOf(resultSet.getLong(ID)), resultSet.getInt(CORE));
                        }
                        return map;
                    }
                });
                logger.debug("执行querySql : {}, params : {}，共{}条", querySql, paramsToUse, supplierCoreStatusMap.size());
                List<Map<String, Object>> resultFromEs = updateSupplierCoreStatus(supplierCoreStatusMap);
                // 保存到es
                batchExecute(resultFromEs);
            }
        }
    }

    private List<Map<String, Object>> updateSupplierCoreStatus(Map<String, Object> supplierCoreStatusMap) {
        Properties properties = elasticClient.getProperties();
        SearchResponse searchResponse = elasticClient.getTransportClient()
                .prepareSearch(properties.getProperty("cluster.index"))
                .setTypes(properties.getProperty("cluster.type.supplier"))
                .setQuery(QueryBuilders.termsQuery(ID, supplierCoreStatusMap.keySet()))
                .setFrom(0)
                .setSize(supplierCoreStatusMap.size())
                .execute()
                .actionGet();

        SearchHits hits = searchResponse.getHits();
        List<Map<String, Object>> suppliers = new ArrayList<>();
        long totalHits = hits.getTotalHits();
        if (totalHits > 0) {
            for (SearchHit searchHit : hits.hits()) {
                Map<String, Object> source = searchHit.getSource();
                source.put(CORE, supplierCoreStatusMap.get(source.get(ID)));
                refresh(source);
                suppliers.add(source);
            }
        }
        return suppliers;
    }

    private void syncSupplierDataService(Timestamp lastSyncTime) {
        logger.info("同步供应商开始");
        doInsertedSupplierData(lastSyncTime);
        doUpdatedSupplierData(lastSyncTime);
        logger.info("同步供应商结束");
    }

    private void doInsertedSupplierData(Timestamp lastSyncTime) {
        String countInsertedSql = "SELECT count(1) FROM t_reg_company trc JOIN t_reg_user tru ON trc.id = tru.COMPANY_ID and trc.type = 13 and trc.COMP_TYPE IS NOT NULL WHERE trc.CREATE_DATE > ?";
        String queryInsertedSql = "SELECT\n"
                                  + "trc.id,\n"
                                  + "   trc.`NAME` AS companyName,\n"
                                  + "   trc.NAME_ENGLISH AS companyNameEn,\n"
                                  + "   trc.AREA AS area,\n"
                                  + "   trc.ZONE_STR AS zoneStr,\n"
                                  + "   trc.ADDRESS AS address,\n"
                                  + "   trc.ADDRESS_EN AS addressEn,\n"
                                  + "   trc.AUTH_STATUS AS authStatus,\n"
                                  + "   trc.AUTH_STATUS2 AS authStatus2,\n"
                                  + "   trc.AUTH_TYPE AS authType,\n"
                                  + "   trc.AUTH_CODE_ID AS authCodeId,\n"
                                  + "   trc.AUTH_TIME AS authTime,\n"
                                  + "   trc.AUTHEN_NUMBER AS authenNumber,\n"
                                  + "   trc.`CODE` AS code,\n"
                                  + "   trc.BIDAUTH_EXPIRES AS bidAuthExpires,\n"
                                  + "   trc.BIDAUTH_FTIME AS bidAuthFtime,\n"
                                  + "   trc.BIDAUTH_STATUS AS bidAuthStatus,\n"
                                  + "   trc.BIDAUTH_TIME AS bidAuthTime,\n"
                                  + "   trc.company_site AS companySite,\n"
                                  + "   trc.WWW_STATION AS wwwStation,\n"
                                  + "   trc.COMP_TYPE AS companyType,\n"
                                  + "   trcct.`NAME` AS companyTypStr,\n"
                                  + "   trc.FOUNDED_DATE AS foundedDate,\n"
                                  + "   trc.FUND AS fund,\n"
                                  + "   trc.FUNDUNIT AS fundUnit,\n"
                                  + "   trc.INDUSTRY AS industryCode,\n"
                                  + "   trc.INDUSTRY_STR AS industryStr,\n"
                                  + "   trc.MAIN_PRODUCT AS mainProduct,\n"
                                  + "   trc.WORKPATTERN AS workPattern,\n"
                                  + "   trc.TEL AS tel,\n"
                                  + "   trc.CONTACT AS contact,\n"
                                  + "   trc.CONTACT_EN AS contactEn,\n"
                                  + "   trc.CREATE_DATE AS createTime,\n"
                                  + "   trc.UPDATE_TIME AS updateTime,\n"
                                  + "   trc.WEB_TYPE AS webType,\n"
                                  + "   trc.TENANT_ID AS tenantId,\n"
                                  + "   tru.LOGIN_NAME AS loginName,\n"
                                  + "   tru.`NAME` AS supplierName,\n"
                                  + "   tru.MOBILE AS mobile,\n"
                                  + "   IFNULL(tucs.CREDIT_MEDAL_STATUS,0) AS core\n"
                                  + "FROM\n"
                                  + "   t_reg_company trc\n"
                                  + "JOIN t_reg_user tru ON trc.id = tru.COMPANY_ID and trc.type = 13 and trc.COMP_TYPE IS NOT NULL\n"
                                  + "JOIN t_uic_company_status tucs ON trc.id = tucs.COMP_ID\n"
                                  + "LEFT JOIN t_reg_code_comp_type trcct ON trc.COMP_TYPE = trcct.ID\n"
                                  + "WHERE trc.CREATE_DATE > ?\n"
                                  + "GROUP BY trc.ID\n"
                                  + "LIMIT ?,?";
        doSyncSupplierData(countInsertedSql, queryInsertedSql, lastSyncTime);
    }

    private void doUpdatedSupplierData(Timestamp lastSyncTime) {
        String countUpdatedSql = "SELECT count(1) FROM t_reg_company trc JOIN t_reg_user tru ON trc.id = tru.COMPANY_ID and trc.type = 13 and trc.COMP_TYPE IS NOT NULL WHERE trc.UPDATE_TIME > ?";
        String queryUpdatedSql = "SELECT\n"
                                 + "   trc.id,\n"
                                 + "   trc.`NAME` AS companyName,\n"
                                 + "   trc.NAME_ENGLISH AS companyNameEn,\n"
                                 + "   trc.AREA AS area,\n"
                                 + "   trc.ZONE_STR AS zoneStr,\n"
                                 + "   trc.ADDRESS AS address,\n"
                                 + "   trc.ADDRESS_EN AS addressEn,\n"
                                 + "   trc.AUTH_STATUS AS authStatus,\n"
                                 + "   trc.AUTH_STATUS2 AS authStatus2,\n"
                                 + "   trc.AUTH_TYPE AS authType,\n"
                                 + "   trc.AUTH_CODE_ID AS authCodeId,\n"
                                 + "   trc.AUTH_TIME AS authTime,\n"
                                 + "   trc.AUTHEN_NUMBER AS authenNumber,\n"
                                 + "   trc.`CODE` AS code,\n"
                                 + "   trc.BIDAUTH_EXPIRES AS bidAuthExpires,\n"
                                 + "   trc.BIDAUTH_FTIME AS bidAuthFtime,\n"
                                 + "   trc.BIDAUTH_STATUS AS bidAuthStatus,\n"
                                 + "   trc.BIDAUTH_TIME AS bidAuthTime,\n"
                                 + "   trc.company_site AS companySite,\n"
                                 + "   trc.WWW_STATION AS wwwStation,\n"
                                 + "   trc.COMP_TYPE AS companyType,\n"
                                 + "   trcct.`NAME` AS companyTypStr,\n"
                                 + "   trc.FOUNDED_DATE AS foundedDate,\n"
                                 + "   trc.FUND AS fund,\n"
                                 + "   trc.FUNDUNIT AS fundUnit,\n"
                                 + "   trc.INDUSTRY AS industryCode,\n"
                                 + "   trc.INDUSTRY_STR AS industryStr,\n"
                                 + "   trc.MAIN_PRODUCT AS mainProduct,\n"
                                 + "   trc.WORKPATTERN AS workPattern,\n"
                                 + "   trc.TEL AS tel,\n"
                                 + "   trc.CONTACT AS contact,\n"
                                 + "   trc.CONTACT_EN AS contactEn,\n"
                                 + "   trc.CREATE_DATE AS createTime,\n"
                                 + "   trc.UPDATE_TIME AS updateTime,\n"
                                 + "   trc.WEB_TYPE AS webType,\n"
                                 + "   trc.TENANT_ID AS tenantId,\n"
                                 + "   tru.LOGIN_NAME AS loginName,\n"
                                 + "   tru.`NAME` AS supplierName,\n"
                                 + "   tru.MOBILE AS mobile,\n"
                                 + "   IFNULL(tucs.CREDIT_MEDAL_STATUS,0) AS core\n"
                                 + "FROM\n"
                                 + "   t_reg_company trc\n"
                                 + "JOIN t_reg_user tru ON trc.id = tru.COMPANY_ID and trc.type = 13 and trc.COMP_TYPE IS NOT NULL\n"
                                 + "JOIN t_reg_code_comp_type trcct ON trc.COMP_TYPE = trcct.ID\n"
                                 + "LEFT JOIN t_uic_company_status tucs ON trc.id = tucs.COMP_ID\n"
                                 + "WHERE trc.UPDATE_TIME > ?\n"
                                 + "GROUP BY trc.ID\n"
                                 + "LIMIT ?,?";
        doSyncSupplierData(countUpdatedSql, queryUpdatedSql, lastSyncTime);
    }

    private void doSyncSupplierData(String countSql, String querySql, Timestamp createTime) {
        List<Object> params = new ArrayList<>();
        params.add(createTime);
        long count = DBUtil.count(centerDataSource, countSql, params);
        logger.debug("执行countSql : {}, params : {}，共{}条", countSql, params, count);
        if (count > 0) {
            for (long i = 0; i < count; i += pageSize) {
                List<Object> paramsToUse = appendToParams(params, i);
                // 查出符合条件的供应商
                List<Map<String, Object>> resultToExecute = DBUtil.query(centerDataSource, querySql, paramsToUse);
                logger.debug("执行querySql : {}, params : {}，共{}条", querySql, paramsToUse, resultToExecute.size());
                Set<Long> supplierIds = new HashSet<>();
                for (Map<String, Object> result : resultToExecute) {
                    if (StringUtils.isEmpty(result.get(ZONE_STR))) {
                        if (!StringUtils.isEmpty(result.get(AREA))) {
                            supplierIds.add((Long) result.get(ID));
                        }
                    } else {
                        result.put(AREA_STR, result.get(ZONE_STR));
                    }
                    refresh(result);
                }

                // 添加区域
                appendAreaStrToResult(resultToExecute, supplierIds);
                // 添加诚信等级
                appendCreditToResult(resultToExecute, supplierIds);
                // 保存到es
                batchExecute(resultToExecute);
            }
        }
    }

    private void refresh(Map<String, Object> resultToUse) {
        resultToUse.remove(AREA);
        resultToUse.put(AUTH_CODE_ID, convertToString(resultToUse.get(AUTH_CODE_ID)));
        resultToUse.put(AUTHEN_NUMBER, convertToString(resultToUse.get(AUTHEN_NUMBER)));
        resultToUse.put(CODE, convertToString(resultToUse.get(CODE)));
        resultToUse.put(FUND, convertToString(resultToUse.get(FUND)));
        resultToUse.put(TENANT_ID, convertToString(resultToUse.get(TENANT_ID)));
        resultToUse.put(MOBILE, convertToString(resultToUse.get(MOBILE)));
        resultToUse.put(TEL, convertToString(resultToUse.get(TEL)));
        resultToUse.put(ID, convertToString(resultToUse.get(ID)));
        resultToUse.put(SyncTimeUtil.SYNC_TIME, SyncTimeUtil.getCurrentDate());
        // 企业空间
        String loginName = convertToString(resultToUse.get(LOGIN_NAME));
        if (loginName != null) {
            String enterpriseSpace = MessageFormat.format(enterpriseSpaceFormat, loginName.toLowerCase(), loginName);
            resultToUse.put(ENTERPRISE_SPACE, enterpriseSpace);
        }
    }

    public String convertToString(Object value) {
        if (value == null) {
            return null;
        } else {
            return String.valueOf(value);
        }
    }

    private void appendCreditToResult(List<Map<String, Object>> resultToExecute, Set<Long> supplierIds) {
        if (supplierIds.size() > 0) {
            Map<Long, Object> creditMap = queryCredit(supplierIds);
            for (Map<String, Object> result : resultToExecute) {
                Long supplierId = Long.valueOf(String.valueOf(result.get(ID)));
                Object creditRating = creditMap.get(supplierId);
                // 如果没有诚信等级，就置为0
                result.put(CREDIT_RATING, (creditRating == null ? 0 : creditRating));
            }
        }
    }

    private Map<Long, Object> queryCredit(Set<Long> supplierIds) {
        String queryCreditTemplate = "SELECT COMPANY_ID as id, RATING as creditRating FROM credit_score WHERE id in (%s)";
        String queryCreditSql = String.format(queryCreditTemplate, StringUtils.collectionToCommaDelimitedString(supplierIds));
        List<Map<String, Object>> query = DBUtil.query(creditDataSource, queryCreditSql, null);
        Map<Long, Object> creditMap = new HashMap<>();
        for (Map<String, Object> map : query) {
            creditMap.put((Long) map.get(ID), map.get(CREDIT_RATING));
        }
        return creditMap;
    }

    private void appendAreaStrToResult(List<Map<String, Object>> resultToExecute, Set<Long> supplierIds) {
        if (supplierIds.size() > 0) {
            Map<Long, Object> areaMap = queryArea(supplierIds);
            for (Map<String, Object> result : resultToExecute) {
                if (StringUtils.isEmpty(result.get(ZONE_STR))) {
                    Long supplierId = Long.valueOf(String.valueOf(result.get(ID)));
                    result.put(AREA_STR, areaMap.get(supplierId));
                } else {
                    result.put(AREA_STR, result.get(ZONE_STR));
                }
            }
        }
    }

    private Map<Long, Object> queryArea(Set<Long> supplierIds) {
        String queryAreaTemplate = "SELECT\n"
                                   + "   t3.ID AS id,\n"
                                   + "   t3.AREA AS area,\n"
                                   + "   t3.CITY AS city,\n"
                                   + "   tcrd.`VALUE` AS county\n"
                                   + "FROM\n"
                                   + "   (\n"
                                   + "      SELECT\n"
                                   + "         t2.ID, t2.AREA, t2.COUNTY, tcrd.`VALUE` AS CITY\n"
                                   + "      FROM\n"
                                   + "         (\n"
                                   + "            SELECT\n"
                                   + "               t1.ID, t1.CITY, t1.COUNTY, tcrd.`VALUE` AS AREA\n"
                                   + "            FROM\n"
                                   + "               (SELECT ID, COUNTRY, AREA, CITY, COUNTY FROM t_reg_company WHERE ID IN (%s) AND TYPE = 13 AND COUNTRY IS NOT NULL) t1\n"
                                   + "            JOIN t_reg_center_dict tcrd ON t1.AREA = tcrd.`KEY`\n"
                                   + "            WHERE\n"
                                   + "               tcrd.TYPE = 'country'\n"
                                   + "         ) t2\n"
                                   + "      LEFT JOIN t_reg_center_dict tcrd ON t2.CITY = tcrd.`KEY`\n"
                                   + "      WHERE\n"
                                   + "         tcrd.TYPE = 'country' OR tcrd.TYPE IS NULL\n"
                                   + "   ) t3\n"
                                   + "LEFT JOIN t_reg_center_dict tcrd ON t3.COUNTY = tcrd.`KEY`\n"
                                   + "WHERE\n"
                                   + "   tcrd.TYPE = 'country' OR tcrd.TYPE IS NULL";

        String queryAreaSql = String.format(queryAreaTemplate, StringUtils.collectionToCommaDelimitedString(supplierIds));
        List<Map<String, Object>> query = DBUtil.query(centerDataSource, queryAreaSql, null);
        Map<Long, Object> areaMap = new HashMap<>();
        for (Map<String, Object> map : query) {
            Object area = map.get(AREA);
            Object city = map.get(CITY);
            Object county = map.get(COUNTY);
            String areaStr = null;
            if (city == null && county == null) {
                areaStr = String.valueOf(area);
            } else if (area.equals(city)) {
                areaStr = String.valueOf(city) + String.valueOf(county);
            } else {
                areaStr = String.valueOf(area) + String.valueOf(city) + String.valueOf(county);
            }
            areaMap.put((Long) map.get(ID), areaStr);
        }
        return areaMap;
    }


    protected void batchExecute(List<Map<String, Object>> resultsToUpdate) {
//        System.out.println("size : " + resultsToUpdate.size());
//        for (Map<String, Object> map : resultsToUpdate) {
//            System.out.println(map);
//        }
        if (!CollectionUtils.isEmpty(resultsToUpdate)) {
            BulkRequestBuilder bulkRequest = elasticClient.getTransportClient().prepareBulk();
            for (Map<String, Object> result : resultsToUpdate) {
                bulkRequest.add(elasticClient.getTransportClient()
                                        .prepareIndex(elasticClient.getProperties().getProperty("cluster.index"),
                                                      elasticClient.getProperties().getProperty("cluster.type.supplier"),
                                                      String.valueOf(result.get(ID)))
                                        .setSource(JSON.toJSONString(result, new ValueFilter() {
                                            @Override
                                            public Object process(Object object, String propertyName, Object propertyValue) {
                                                if (propertyValue instanceof java.util.Date) {
                                                    return new DateTime(propertyValue).toString(SyncTimeUtil.DATE_TIME_PATTERN);
                                                } else {
                                                    return propertyValue;
                                                }
                                            }
                                        })));
            }

            BulkResponse response = bulkRequest.execute().actionGet();
            if (response.hasFailures()) {
                logger.error(response.buildFailureMessage());
            }
        }
    }
}
