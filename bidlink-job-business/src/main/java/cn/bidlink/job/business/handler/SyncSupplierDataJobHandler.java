package cn.bidlink.job.business.handler;

import cn.bidlink.job.business.utils.AreaUtil;
import cn.bidlink.job.business.utils.RegionUtil;
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
import org.elasticsearch.common.unit.TimeValue;
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
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.text.MessageFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static cn.bidlink.job.common.utils.DBUtil.query;


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
    @Qualifier("enterpriseSpaceDataSource")
    private DataSource enterpriseSpaceDataSource;

    @Autowired
    @Qualifier("centerDataSource")
    private DataSource centerDataSource;

//    @Autowired
//    @Qualifier("synergyDataSource")
//    protected DataSource synergyDataSource;

    @Autowired
    @Qualifier("ycDataSource")
    private DataSource ycDataSource;

    @Value("${enterpriseSpaceFormat}")
    private String enterpriseSpaceFormat;

    @Value("${enterpriseSpaceDetailFormat}")
    private String enterpriseSpaceDetailFormat;

    private String ID                          = "id";
    private String CORE                        = "core";
    private String AREA_STR                    = "areaStr";
    private String ZONE_STR                    = "zoneStr";
    private String AREA                        = "area";
    private String CITY                        = "city";
    private String COUNTY                      = "county";
    private String CREDIT_RATING               = "creditRating";
    private String AUTH_CODE_ID                = "authCodeId";
    private String AUTHEN_NUMBER               = "authenNumber";
    private String CODE                        = "code";
    private String FUND                        = "fund";
    private String TENANT_ID                   = "tenantId";
    private String MOBILE                      = "mobile";
    private String TEL                         = "tel";
    private String LOGIN_NAME                  = "loginName";
    private String ENTERPRISE_SPACE            = "enterpriseSpace";
    private String ENTERPRISE_SPACE_DETAIL     = "enterpriseSpaceDetail";
    private String ENTERPRISE_SPACE_ACTIVE     = "enterpriseSpaceActive";
    private String INDUSTRY_CODE               = "industryCode";
    private String INDUSTRY_STR                = "industryStr";
    private String TOTAL_PURCHASE_PROJECT      = "totalPurchaseProject";
    private String TOTAL_BID_PROJECT           = "totalBidProject";
    private String TOTAL_DEAL_PURCHASE_PROJECT = "totalDealPurchaseProject";
    private String TOTAL_DEAL_BID_PROJECT      = "totalDealBidProject";
    private String TOTAL_PRODUCT               = "totalProduct";
    private String TOTAL_COOPERATED_PURCHASER  = "totalCooperatedPurchaser";
    private String REGION                      = "region";
    private String COMPANY_NAME                = "companyName";
    private String COMPANY_NAME_NOT_ANALYZED   = "companyNameNotAnalyzed";

    // 两位有效数字，四舍五入
    private final DecimalFormat format = new DecimalFormat("0.00");

    // 行业编号正则
    private final Pattern number = Pattern.compile("\\d+");

    private Map<String, String> industryCodeMap = new HashMap<>();

    @Override
    public void afterPropertiesSet() throws Exception {
        pageSize = 1000;
        industryCodeMap = initIndustryCodeMap();
    }

    private Map<String, String> initIndustryCodeMap() {
        String queryIndustrySql = "SELECT code, name_cn, name_en, type from t_reg_code_trade_class";
        return DBUtil.query(centerDataSource, queryIndustrySql, null, new DBUtil.ResultSetCallback<Map<String, String>>() {
            @Override
            public Map<String, String> execute(ResultSet resultSet) throws SQLException {
                Map<String, String> map = new HashMap<String, String>();
                while (resultSet.next()) {
                    String code = resultSet.getString("code");
                    String nameCn = resultSet.getString("name_cn");
                    String nameEn = resultSet.getString("name_en");
                    int type = resultSet.getInt("type");
                    if (type == 1) {
                        map.put(code, nameCn);
                    } else {
                        map.put(code, nameEn);
                    }
                }
                return map;
            }
        });
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
        syncEnterpriseSpaceDataService(lastSyncTime);
        syncSupplierStatService(lastSyncTime);
    }

    /**
     * 同步企业空间
     *
     * @param lastSyncTime
     */
    private void syncEnterpriseSpaceDataService(Timestamp lastSyncTime) {
        if (SyncTimeUtil.GMT_TIME.equals(lastSyncTime)) {
            logger.info("首次同步，忽略同步企业空间");
            return;
        }

        logger.info("同步企业空间开始");
        String countInsertedSql = "SELECT count(1) FROM space_info WHERE CREATE_DATE > ?";
        String queryInsertedSql = "SELECT COMPANY_ID AS id, STATE AS enterpriseSpaceActive FROM space_info WHERE CREATE_DATE > ? LIMIT ?, ?";
        doSyncEnterpriseSpaceService(countInsertedSql, queryInsertedSql, lastSyncTime);

        String countUpdatedSql = "SELECT count(1) FROM space_info WHERE MODIFY_DATE > ?";
        String queryUpdatedSql = "SELECT COMPANY_ID AS id, STATE AS enterpriseSpaceActive FROM space_info WHERE MODIFY_DATE > ? LIMIT ?, ?";
        doSyncEnterpriseSpaceService(countUpdatedSql, queryUpdatedSql, lastSyncTime);
        logger.info("同步企业空间结束");
    }

    private void doSyncEnterpriseSpaceService(String countSql, String querySql, Timestamp lastSyncTime) {
        List<Object> params = new ArrayList<>();
        params.add(lastSyncTime);
        long count = DBUtil.count(enterpriseSpaceDataSource, countSql, params);
        logger.debug("执行countSql : {}, params : {}，共{}条", countSql, params, count);
        if (count > 0) {
            for (long i = 0; i < count; i += pageSize) {
                List<Object> paramsToUse = appendToParams(params, i);
                // 查出符合条件的供应商
                Map<String, Object> enterpriseSpaceInfoMap = query(enterpriseSpaceDataSource, querySql, paramsToUse, new DBUtil.ResultSetCallback<Map<String, Object>>() {
                    @Override
                    public Map<String, Object> execute(ResultSet resultSet) throws SQLException {
                        Map<String, Object> map = new HashMap<>();
                        while (resultSet.next()) {
                            map.put(String.valueOf(resultSet.getLong(ID)), resultSet.getInt(ENTERPRISE_SPACE_ACTIVE));
                        }
                        return map;
                    }
                });
                logger.debug("执行querySql : {}, params : {}，共{}条", querySql, paramsToUse, enterpriseSpaceInfoMap.size());
                List<Map<String, Object>> resultFromEs = updateEnterpriseSpaceActive(enterpriseSpaceInfoMap);
                // 保存到es
                batchExecute(resultFromEs);
            }
        }
    }

    private List<Map<String, Object>> updateEnterpriseSpaceActive(Map<String, Object> enterpriseSpaceInfoMap) {
        Properties properties = elasticClient.getProperties();
        SearchResponse searchResponse = elasticClient.getTransportClient()
                .prepareSearch(properties.getProperty("cluster.index"))
                .setTypes(properties.getProperty("cluster.type.supplier"))
                .setQuery(QueryBuilders.termsQuery(ID, enterpriseSpaceInfoMap.keySet()))
                .setFrom(0)
                .setSize(enterpriseSpaceInfoMap.size())
                .execute()
                .actionGet();

        SearchHits hits = searchResponse.getHits();
        List<Map<String, Object>> suppliers = new ArrayList<>();
        long totalHits = hits.getTotalHits();
        if (totalHits > 0) {
            for (SearchHit searchHit : hits.hits()) {
                Map<String, Object> source = searchHit.getSource();
                Object enterpriseSpaceActive = enterpriseSpaceInfoMap.get(source.get(ID));
                doUpdateEnterpriseSpace(source, enterpriseSpaceActive);
                refresh(source);
                suppliers.add(source);
            }
        }
        return suppliers;
    }

    private void doUpdateEnterpriseSpace(Map<String, Object> source, Object enterpriseSpaceActive) {
        if (enterpriseSpaceActive == null) {
            // 如果没有企业空间，就置为0
            source.put(ENTERPRISE_SPACE_ACTIVE, 0);
        } else {
            source.put(ENTERPRISE_SPACE_ACTIVE, enterpriseSpaceActive);
            String loginName = convertToString(source.get(LOGIN_NAME));
            if (loginName != null) {
                // 企业空间页面
                String enterpriseSpace = MessageFormat.format(enterpriseSpaceFormat, loginName.toLowerCase(), loginName);
                source.put(ENTERPRISE_SPACE, enterpriseSpace);
                // 企业诚信等级页面
                String enterpriseSpaceDetail = MessageFormat.format(enterpriseSpaceDetailFormat, loginName.toLowerCase(), loginName);
                source.put(ENTERPRISE_SPACE_DETAIL, enterpriseSpaceDetail);
            }
        }
    }

    /**
     * 同步核心供状态
     *
     * @param lastSyncTime
     */
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
                Map<String, Object> supplierCoreStatusMap = query(centerDataSource, querySql, paramsToUse, new DBUtil.ResultSetCallback<Map<String, Object>>() {
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

    /**
     * 同步供应商的基本信息
     *
     * @param lastSyncTime
     */
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
//                                  + "   trc.INDUSTRY_STR AS industryStr,\n"
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
                                  + "LEFT JOIN t_uic_company_status tucs ON trc.id = tucs.COMP_ID\n"
                                  + "LEFT JOIN t_reg_code_comp_type trcct ON trc.COMP_TYPE = trcct.ID\n"
                                  + "WHERE trc.CREATE_DATE > ?\n"
                                  + "GROUP BY trc.ID\n"
                                  + "LIMIT ?,?";
        doSyncSupplierData(countInsertedSql, queryInsertedSql, lastSyncTime, true);
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
//                                 + "   trc.INDUSTRY_STR AS industryStr,\n"
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
                                 + "LEFT JOIN t_uic_company_status tucs ON trc.id = tucs.COMP_ID\n"
                                 + "LEFT JOIN t_reg_code_comp_type trcct ON trc.COMP_TYPE = trcct.ID\n"
                                 + "WHERE trc.UPDATE_TIME > ?\n"
                                 + "GROUP BY trc.ID\n"
                                 + "LIMIT ?,?";
        doSyncSupplierData(countUpdatedSql, queryUpdatedSql, lastSyncTime, false);
    }

    private void doSyncSupplierData(String countSql, String querySql, Timestamp createTime, boolean insert) {
        List<Object> params = new ArrayList<>();
        params.add(createTime);
        long count = DBUtil.count(centerDataSource, countSql, params);
        logger.debug("执行countSql : {}, params : {}，共{}条", countSql, params, count);
        if (count > 0) {
            for (long i = 0; i < count; i += pageSize) {
                List<Object> paramsToUse = appendToParams(params, i);
                // 查出符合条件的供应商
                List<Map<String, Object>> resultToExecute = query(centerDataSource, querySql, paramsToUse);
                logger.debug("执行querySql : {}, params : {}，共{}条", querySql, paramsToUse, resultToExecute.size());
                Set<Long> supplierIds = new HashSet<>();
                for (Map<String, Object> result : resultToExecute) {
                    if (!StringUtils.isEmpty(result.get(AREA))) {
                        supplierIds.add((Long) result.get(ID));
                    }
                    refresh(result);
                }

                if (insert) {
                    // 添加区域
                    appendAreaStrToResult(resultToExecute, supplierIds);
                }
                // 添加诚信等级
                appendCreditToResult(resultToExecute, supplierIds);
                // 添加企业空间
                appendEnterpriseSpaceToResult(resultToExecute, supplierIds);
                // 保存到es
                batchExecute(resultToExecute);
            }
        }
    }

    private void refresh(Map<String, Object> resultToUse) {
        // 添加不分词字段
        resultToUse.put(COMPANY_NAME_NOT_ANALYZED, resultToUse.get(COMPANY_NAME));
        // 处理所在地区
        Object areaObj = resultToUse.get(AREA);
        if (areaObj != null) {
            String areaCode = String.valueOf(areaObj);
            if (areaCode != null && areaCode.length() > 2) {
                resultToUse.put(REGION, RegionUtil.regionMap.get(areaCode.substring(0, 2)));
            }
        }
        resultToUse.remove(AREA);
        // 重置zoneStr，使得zoneStr和areaStr一致
        resultToUse.put(ZONE_STR, resultToUse.get(AREA_STR));
        // 重置注册资金，四舍五入，保留两位有效数字
        Object value = resultToUse.get(FUND);
        if (value != null && value instanceof BigDecimal) {
            resultToUse.put(FUND, this.format.format(value));
        }
        // 重置行业
        Object industryCodeObj = resultToUse.get(INDUSTRY_CODE);
        if (industryCodeObj != null) {
            Matcher matcher = number.matcher((String) industryCodeObj);
            if (matcher.find()) {
                String industryCode = matcher.group();
                resultToUse.put(INDUSTRY_STR, industryCodeMap.get(industryCode));
            }
        }
        // 将Long转为String，防止前端js精度溢出
        resultToUse.put(AUTH_CODE_ID, convertToString(resultToUse.get(AUTH_CODE_ID)));
        resultToUse.put(AUTHEN_NUMBER, convertToString(resultToUse.get(AUTHEN_NUMBER)));
        resultToUse.put(CODE, convertToString(resultToUse.get(CODE)));
        resultToUse.put(TENANT_ID, convertToString(resultToUse.get(TENANT_ID)));
        resultToUse.put(MOBILE, convertToString(resultToUse.get(MOBILE)));
        resultToUse.put(TEL, convertToString(resultToUse.get(TEL)));
        resultToUse.put(ID, convertToString(resultToUse.get(ID)));
        resultToUse.put(SyncTimeUtil.SYNC_TIME, SyncTimeUtil.getCurrentDate());
    }

    private String convertToString(Object value) {
        if (value == null) {
            return null;
        } else {
            return String.valueOf(value);
        }
    }


    private void appendEnterpriseSpaceToResult(List<Map<String, Object>> resultToExecute, Set<Long> supplierIds) {
        if (supplierIds.size() > 0) {
            Map<Long, Object> enterpriseSpaceInfoMap = queryEnterpriseSpaceInfo(supplierIds);
            for (Map<String, Object> result : resultToExecute) {
                Long supplierId = Long.valueOf(String.valueOf(result.get(ID)));
                Object enterpriseSpaceActive = enterpriseSpaceInfoMap.get(supplierId);
                doUpdateEnterpriseSpace(result, enterpriseSpaceActive);
            }
        }
    }

    private Map<Long, Object> queryEnterpriseSpaceInfo(Set<Long> supplierIds) {
        String queryEnterpriseSpaceInfoTemplate = "SELECT COMPANY_ID AS id,STATE as enterpriseSpaceActive FROM space_info WHERE STATE = 1 AND COMPANY_ID in (%s)";
        String queryEnterpriseSpaceInfoSql = String.format(queryEnterpriseSpaceInfoTemplate, StringUtils.collectionToCommaDelimitedString(supplierIds));
        List<Map<String, Object>> query = query(enterpriseSpaceDataSource, queryEnterpriseSpaceInfoSql, null);
        Map<Long, Object> enterpriseSpaceInfoMap = new HashMap<>();
        for (Map<String, Object> map : query) {
            enterpriseSpaceInfoMap.put((Long) map.get(ID), map.get(ENTERPRISE_SPACE_ACTIVE));
        }
        return enterpriseSpaceInfoMap;
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
        String queryCreditTemplate = "SELECT COMPANY_ID as id, RATING as creditRating FROM credit_score WHERE COMPANY_ID in (%s)";
        String queryCreditSql = String.format(queryCreditTemplate, StringUtils.collectionToCommaDelimitedString(supplierIds));
        List<Map<String, Object>> query = query(creditDataSource, queryCreditSql, null);
        Map<Long, Object> creditMap = new HashMap<>();
        for (Map<String, Object> map : query) {
            creditMap.put((Long) map.get(ID), map.get(CREDIT_RATING));
        }
        return creditMap;
    }

    private void appendAreaStrToResult(List<Map<String, Object>> resultToExecute, Set<Long> supplierIds) {
        if (supplierIds.size() > 0) {
            Map<Long, Object> areaMap = AreaUtil.queryArea(centerDataSource, supplierIds);
            for (Map<String, Object> result : resultToExecute) {
                Long supplierId = Long.valueOf(String.valueOf(result.get(ID)));
                Object value = areaMap.get(supplierId);
                result.put(AREA_STR, value);
                // 重置zoneStr，使得zoneStr和areaStr一致
                result.put(ZONE_STR, value);
            }
        }
    }

    /**
     * 同步供应商参与的项目统计
     * <p>
     * 注意：每次统计需要覆盖之前的统计
     *
     * @param lastSyncTime
     */
    private void syncSupplierStatService(Timestamp lastSyncTime) {
        logger.info("同步供应商参与的项目统计开始");
        Properties properties = elasticClient.getProperties();
        int pageSizeToUse = 2 * pageSize;
        SearchResponse scrollResp = elasticClient.getTransportClient().prepareSearch(properties.getProperty("cluster.index"))
                .setTypes(properties.getProperty("cluster.type.supplier"))
                .setScroll(new TimeValue(60000))
                .setSize(pageSizeToUse)
                .get();

        int pageNumberToUse = 0;
        do {
            SearchHit[] searchHits = scrollResp.getHits().hits();
            Set<String> supplierIds = new HashSet<>();
            List<Map<String, Object>> resultFromEs = new ArrayList<>();
            for (SearchHit searchHit : searchHits) {
                supplierIds.add(searchHit.getId());
                resultFromEs.add(searchHit.getSource());
            }

            String supplierIdToString = StringUtils.collectionToCommaDelimitedString(supplierIds);
            // 添加已参与项目统计
            appendSupplierProjectStat(resultFromEs, supplierIdToString);
            // 添加成交项目统计
            appendSupplierDealProjectStat(resultFromEs, supplierIdToString);
            // 添加合作采购商统计
            appendCooperatedPurchaserState(resultFromEs, supplierIdToString);
            // 添加发布产品
            appendSupplierProductStat(resultFromEs, supplierIdToString);

            // 保存到es
            batchExecute(resultFromEs);
            scrollResp = elasticClient.getTransportClient().prepareSearchScroll(scrollResp.getScrollId())
                    .setScroll(new TimeValue(60000))
                    .execute().actionGet();
            pageNumberToUse++;
        } while (scrollResp.getHits().getHits().length != 0);
        logger.info("同步供应商参与的项目统计结束");
    }

    /**
     * 添加已参与项目统计
     *
     * @param resultFromEs
     * @param supplierIds
     */
    private void appendSupplierProjectStat(List<Map<String, Object>> resultFromEs, String supplierIds) {
        // 采购项目
        String queryPurchaseProjectSqlTemplate = "SELECT\n"
                                                 + "   COUNT(1),\n"
                                                 + "   supplier_id\n"
                                                 + "FROM\n"
                                                 + "   bmpfjz_supplier_project_bid where supplier_bid_status in (2,3,6,7) AND supplier_id in (%s)\n"
                                                 + "GROUP BY\n"
                                                 + "   supplier_id";
        Map<String, Long> purchaseProjectStat = getSupplierStatMap(ycDataSource, queryPurchaseProjectSqlTemplate, supplierIds);
        for (Map<String, Object> source : resultFromEs) {
            Object value = purchaseProjectStat.get(source.get(ID));
            source.put(TOTAL_PURCHASE_PROJECT, (value == null ? 0 : value));
        }

        // 招标项目
        String queryBidProjectSqlTemplate = "select\n"
                                            + "    count(1),\n"
                                            + "    BIDER_ID AS supplierId\n"
                                            + " from\n"
                                            + "    bid\n"
                                            + "    where IS_WITHDRAWBID = 0 AND bider_id in (%s)\n"
                                            + " GROUP BY\n"
                                            + "    BIDER_ID";
        Map<String, Long> bidProjectStat = getSupplierStatMap(ycDataSource, queryBidProjectSqlTemplate, supplierIds);
        for (Map<String, Object> source : resultFromEs) {
            Object value = bidProjectStat.get(source.get(ID));
            source.put(TOTAL_BID_PROJECT, (value == null ? 0 : value));
        }
    }

    /**
     * 添加已成交项目统计
     *
     * @param resultFromEs
     * @param supplierIds
     */
    private void appendSupplierDealProjectStat(List<Map<String, Object>> resultFromEs, String supplierIds) {
        // 采购项目
        String queryPurchaseProjectSqlTemplate = "SELECT\n"
                                                 + "   COUNT(1),\n"
                                                 + "   supplier_id\n"
                                                 + "FROM\n"
                                                 + "   bmpfjz_supplier_project_bid where supplier_bid_status = 6 AND supplier_id in (%s)\n"
                                                 + "GROUP BY\n"
                                                 + "   supplier_id";
        Map<String, Long> purchaseProjectStat = getSupplierStatMap(ycDataSource, queryPurchaseProjectSqlTemplate, supplierIds);
        for (Map<String, Object> source : resultFromEs) {
            Object value = purchaseProjectStat.get(source.get(ID));
            source.put(TOTAL_DEAL_PURCHASE_PROJECT, (value == null ? 0 : value));
        }

        // 招标项目
        String queryBidProjectSqlTemplate = "select\n"
                                            + "    count(1),\n"
                                            + "    BIDER_ID AS supplierId\n"
                                            + " from\n"
                                            + "    bid\n"
                                            + "      WHERE\n"
                                            + "         IS_BID_SUCCESS = 1 AND bider_id in (%s)\n"
                                            + " GROUP BY\n"
                                            + "    BIDER_ID";
        Map<String, Long> bidProjectStat = getSupplierStatMap(ycDataSource, queryBidProjectSqlTemplate, supplierIds);
        for (Map<String, Object> source : resultFromEs) {
            Object value = bidProjectStat.get(source.get(ID));
            source.put(TOTAL_DEAL_BID_PROJECT, (value == null ? 0 : value));
        }
    }

    /**
     * 添加合作采购商统计
     *
     * @param resultFromEs
     * @param supplierIds
     */
    private void appendCooperatedPurchaserState(List<Map<String, Object>> resultFromEs, String supplierIds) {
        String queryCooperatedPurchaserSqlTemplate = "select\n"
                                                     + "   count(1) AS totalCooperatedPurchaser,\n"
                                                     + "   supplier_id AS supplierId\n"
                                                     + "from\n"
                                                     + "   bsm_company_supplier_apply\n"
                                                     + "   WHERE supplier_status in (2,3,4) AND supplier_id in (%s)\n"
                                                     + "GROUP BY\n"
                                                     + "   supplier_id\n";
        Map<String, Long> supplierStat = getSupplierStatMap(ycDataSource, queryCooperatedPurchaserSqlTemplate, supplierIds);
        for (Map<String, Object> source : resultFromEs) {
            Object value = supplierStat.get(source.get(ID));
            source.put(TOTAL_COOPERATED_PURCHASER, (value == null ? 0 : value));
        }
    }

    /**
     * 添加发布产品统计
     *
     * @param resultFromEs
     * @param supplierIds
     */
    private void appendSupplierProductStat(List<Map<String, Object>> resultFromEs, String supplierIds) {
        String querySupplierProductSqlTemplate = "SELECT\n"
                                                 + "   count(1),\n"
                                                 + "   COMPANYID\n"
                                                 + "FROM\n"
                                                 + "   space_product\n"
                                                 + "WHERE\n"
                                                 + "   STATE = 1 AND  COMPANYID in (%s)\n"
                                                 + "GROUP BY\n"
                                                 + "   COMPANYID";
        Map<String, Long> supplierStat = getSupplierStatMap(enterpriseSpaceDataSource, querySupplierProductSqlTemplate, supplierIds);
        for (Map<String, Object> source : resultFromEs) {
            Object value = supplierStat.get(source.get(ID));
            source.put(TOTAL_PRODUCT, (value == null ? 0 : value));
        }
    }

    private Map<String, Long> getSupplierStatMap(DataSource dataSource, String querySqlTemplate, String supplierIds) {
        String querySql = String.format(querySqlTemplate, supplierIds);
        return DBUtil.query(dataSource, querySql, null, new DBUtil.ResultSetCallback<Map<String, Long>>() {
            @Override
            public Map<String, Long> execute(ResultSet resultSet) throws SQLException {
                Map<String, Long> map = new HashMap<>();
                while (resultSet.next()) {
                    long totalProject = resultSet.getLong(1);
                    long supplierId = resultSet.getLong(2);
                    map.put(String.valueOf(supplierId), totalProject);
                }
                return map;
            }
        });
    }

    /**
     * 统计供应商已交易的项目
     */
    private void syncSupplierDealProjectStat() {
//        String queryDealPurchaseProjectSql = "SELECT count(1) AS totalPurchaseProject,supplier_id AS supplierId FROM purchase_supplier_project WHERE deal_status = 1 GROUP BY supplier_id LIMIT ?,?";
//        String countDealPurchaseProjectSql = "SELECT\n"
//                                             + "   count(1)\n"
//                                             + "FROM\n"
//                                             + "   (\n"
//                                             + "SELECT\n"
//                                             + "   count(1),\n"
//                                             + "   supplier_id AS supplierId\n"
//                                             + "FROM\n"
//                                             + "   purchase_supplier_project\n"
//                                             + "WHERE deal_status = 1\n"
//                                             + "GROUP BY\n"
//                                             + "   supplier_id\n"
//                                             + "   ) S";
//        doSyncSupplierStat(synergyDataSource, countDealPurchaseProjectSql, queryDealPurchaseProjectSql, new SupplierStatCallback() {
//            @Override
//            public void execute(Map<String, Object> source, Map supplierStat) {
//                source.put(TOTAL_DEAL_PURCHASE_PROJECT, supplierStat.get(source.get(ID)));
//            }
//        });

    }

    /**
     * 统计供应商参与的项目
     */
    private void syncSupplierProjectStat() {
//        String queryPurchaseProjectSql = "SELECT count(1) AS totalPurchaseProject,supplier_id AS supplierId FROM purchase_supplier_project GROUP BY supplier_id LIMIT ?,?";
//        String countPurchaseProjectSql = "SELECT\n"
//                                         + "   count(1)\n"
//                                         + "FROM\n"
//                                         + "   (\n"
//                                         + "SELECT\n"
//                                         + "   count(1),\n"
//                                         + "   supplier_id AS supplierId\n"
//                                         + "FROM\n"
//                                         + "   purchase_supplier_project\n"
//                                         + "GROUP BY\n"
//                                         + "   supplier_id\n"
//                                         + "LIMIT ?,?"
//                                         + "   ) S";
//        doSyncSupplierStat(synergyDataSource, countPurchaseProjectSql, queryPurchaseProjectSql, new SupplierStatCallback() {
//            @Override
//            public void execute(Map<String, Object> source, Map supplierStat) {
//                source.put(TOTAL_PURCHASE_PROJECT, supplierStat.get(source.get(ID)));
//            }
//        });
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
