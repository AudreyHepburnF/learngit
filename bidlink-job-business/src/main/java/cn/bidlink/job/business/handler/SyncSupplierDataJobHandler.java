package cn.bidlink.job.business.handler;

import cn.bidlink.job.common.constant.BusinessConstant;
import cn.bidlink.job.common.utils.AreaUtil;
import cn.bidlink.job.common.utils.DBUtil;
import cn.bidlink.job.common.utils.ElasticClientUtil;
import cn.bidlink.job.common.utils.SyncTimeUtil;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.annotation.JobHander;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.joda.time.DateTime;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.MessageFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

import static cn.bidlink.job.common.utils.DBUtil.query;


/**
 * @author : <a href="mailto:zikaifeng@ebnew.com">冯子恺</a>
 * @version : Ver 1.0
 * @description :供应商基本信息 {@link SyncSupplierProjectDataJobHandler 供应商项目数据}
 * @date : 2017/11/27
 */
@JobHander(value = "syncSupplierDataJobHandler")
@Service
public class SyncSupplierDataJobHandler extends AbstractSyncSupplierDataJobHandler implements InitializingBean {

    private Integer SYNC_WAY_CREATE = 1;
    private Integer SYNC_WAY_UPDATE = 2;

    @Override
    public void afterPropertiesSet() throws Exception {
        pageSize = 1000;
        industryCodeMap = initIndustryCodeMap();
//        execute();
    }

    private Map<String, String> initIndustryCodeMap() {
        String queryIndustrySql = "SELECT code, name_cn, name_en, type from t_reg_code_trade_class";
        return DBUtil.query(uniregDataSource, queryIndustrySql, null, new DBUtil.ResultSetCallback<Map<String, String>>() {
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
        logger.info("供应商数据同步时间lastSyncTime：" + new DateTime(lastSyncTime).toString("yyyy-MM-dd HH:mm:ss") + "\n"
                + ", syncTime : " + new DateTime(SyncTimeUtil.getCurrentDate()).toString("yyyy-MM-dd HH:mm:ss"));
        syncSupplierDataService(lastSyncTime);
        // FIXME 供应商企业空间是否激活
        syncEnterpriseSpaceDataService(lastSyncTime);
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
//    private void syncSupplierCompanyStatusService(Timestamp lastSyncTime) {
//        if (SyncTimeUtil.GMT_TIME.equals(lastSyncTime)) {
//            logger.info("首次同步，忽略同步供应商状态");
//            return;
//        }
//
//        logger.info("同步供应商状态开始");
//        String countInsertedSql = "SELECT count(1) FROM t_uic_company_status WHERE CREATE_TIME > ?";
//        String queryInsertedSql = "SELECT COMP_ID AS id, CREDIT_MEDAL_STATUS AS core FROM t_uic_company_status WHERE CREATE_TIME > ? GROUP BY COMP_ID ORDER BY CREATE_TIME LIMIT ?, ?";
//        doSyncSupplierCompanyStatus(countInsertedSql, queryInsertedSql, lastSyncTime);
//
//        String countUpdatedSql = "SELECT count(1) FROM t_uic_company_status WHERE UPDATE_TIME > ?";
//        String queryUpdatedSql = "SELECT COMP_ID AS id, CREDIT_MEDAL_STATUS AS core FROM t_uic_company_status WHERE UPDATE_TIME > ? GROUP BY COMP_ID ORDER BY UPDATE_TIME LIMIT ?, ?";
//        doSyncSupplierCompanyStatus(countUpdatedSql, queryUpdatedSql, lastSyncTime);
//        logger.info("同步供应商状态结束");
//    }

//    private void doSyncSupplierCompanyStatus(String countSql, String querySql, Timestamp lastSyncTime) {
//        List<Object> params = new ArrayList<>();
//        params.add(lastSyncTime);
//        long count = DBUtil.count(centerDataSource, countSql, params);
//        logger.debug("执行countSql : {}, params : {}，共{}条", countSql, params, count);
//        if (count > 0) {
//            for (long i = 0; i < count; i += pageSize) {
//                List<Object> paramsToUse = appendToParams(params, i);
//                // 查出符合条件的供应商
//                Map<String, Object> supplierCoreStatusMap = query(centerDataSource, querySql, paramsToUse, new DBUtil.ResultSetCallback<Map<String, Object>>() {
//                    @Override
//                    public Map<String, Object> execute(ResultSet resultSet) throws SQLException {
//                        Map<String, Object> map = new HashMap<>();
//                        while (resultSet.next()) {
//                            map.put(String.valueOf(resultSet.getLong(ID)), resultSet.getInt(CORE));
//                        }
//                        return map;
//                    }
//                });
//                logger.debug("执行querySql : {}, params : {}，共{}条", querySql, paramsToUse, supplierCoreStatusMap.size());
//                List<Map<String, Object>> resultFromEs = updateSupplierCoreStatus(supplierCoreStatusMap);
//                // 保存到es
//                batchExecute(resultFromEs);
//            }
//        }
//    }

//    private List<Map<String, Object>> updateSupplierCoreStatus(Map<String, Object> supplierCoreStatusMap) {
//        Properties properties = elasticClient.getProperties();
//        SearchResponse searchResponse = elasticClient.getTransportClient()
//                .prepareSearch(properties.getProperty("cluster.index"))
//                .setTypes(properties.getProperty("cluster.type.supplier"))
//                .setQuery(QueryBuilders.termsQuery(ID, supplierCoreStatusMap.keySet()))
//                .setFrom(0)
//                .setSize(supplierCoreStatusMap.size())
//                .execute()
//                .actionGet();
//
//        SearchHits hits = searchResponse.getHits();
//        List<Map<String, Object>> suppliers = new ArrayList<>();
//        long totalHits = hits.getTotalHits();
//        if (totalHits > 0) {
//            for (SearchHit searchHit : hits.hits()) {
//                Map<String, Object> source = searchHit.getSource();
//                source.put(CORE, supplierCoreStatusMap.get(source.get(ID)));
//                refresh(source);
//                suppliers.add(source);
//            }
//        }
//        return suppliers;
//    }

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
        String countInsertedSql = "SELECT count(1) FROM t_reg_company trc JOIN t_reg_user tru ON trc.id = tru.COMPANY_ID and trc.type = 13 WHERE trc.CREATE_TIME > ?";
        String queryInsertedSql = "SELECT\n" +
                "\ttrc.id,\n" +
                "\ttrc.`NAME` AS companyName,\n" +
                "\ttrc.NAME_ENGLISH AS companyNameEn,\n" +
                "\ttrc.AREA AS area,\n" +
                "\ttrc.ZONE_STR AS zoneStr,\n" +
                "\ttrc.ADDRESS AS address,\n" +
                "\ttrc.BIDAUTH_EXPIRES AS bidAuthExpires,\n" +
                "\ttrc.BIDAUTH_FTIME AS bidAuthFtime,\n" +
                "\ttrc.BIDAUTH_STATUS AS bidAuthStatus,\n" +
                "\ttrc.BIDAUTH_TIME AS bidAuthTime,\n" +
                "\ttrc.company_site AS companySite,\n" +
                "\ttrc.WWW_STATION AS wwwStation,\n" +
                "\ttrc.COMP_TYPE AS companyType,\n" +
                "\ttrcct.`NAME` AS companyTypStr,\n" +
                "\ttrc.FUND AS fund,\n" +
                "\ttrc.FUNDUNIT AS fundUnit,\n" +
                "\ttrc.INDUSTRY AS industryCode,\n" +
                "\ttrc.company_logo AS companyLogo,\n" +
                "\ttrc.MAIN_PRODUCT AS mainProduct,\n" +
                "\ttrc.WORKPATTERN AS workPattern,\n" +
                "\ttrc.TEL AS tel,\n" +
                "\ttrc.CONTACT AS contact,\n" +
                "\ttrc.CREATE_TIME AS createTime,\n" +
                "\ttrc.UPDATE_TIME AS updateTime,\n" +
                "\ttrc.WEB_TYPE AS webType,\n" +
                "\ttrc.status AS status,\n" +
                "\ttru.LOGIN_NAME AS loginName,\n" +
                "\ttru.`NAME` AS supplierName,\n" +
                "\ttru.MOBILE AS mobile,\n" +
                "\ttru.ID AS userId,\n" +
                "\ttrc.core_status AS coreStatus \n" +
                "FROM\n" +
                "\tt_reg_company trc\n" +
                "\tJOIN t_reg_user tru ON trc.id = tru.COMPANY_ID \n" +
                "\tAND trc.type = 13\n" +
                "\tLEFT JOIN t_reg_code_comp_type trcct ON trc.COMP_TYPE = trcct.ID \n" +
                "WHERE\n" +
                "\ttrc.CREATE_TIME > ? \n" +
                "GROUP BY\n" +
                "\ttrc.ID \n" +
                "\tLIMIT ?,?";
        doSyncSupplierData(countInsertedSql, queryInsertedSql, lastSyncTime, SYNC_WAY_CREATE);
    }

    private void doUpdatedSupplierData(Timestamp lastSyncTime) {
        String countUpdatedSql = "SELECT count(1) FROM t_reg_company trc JOIN t_reg_user tru ON trc.id = tru.COMPANY_ID and trc.type = 13 WHERE trc.UPDATE_TIME > ?";
        String queryUpdatedSql = "SELECT\n"
                + "   trc.id,\n"
                + "   trc.`NAME` AS companyName,\n"
                + "   trc.NAME_ENGLISH AS companyNameEn,\n"
                + "   trc.AREA AS area,\n"
                + "   trc.ZONE_STR AS zoneStr,\n"
                + "   trc.ADDRESS AS address,\n"
                + "   trc.BIDAUTH_EXPIRES AS bidAuthExpires,\n"
                + "   trc.BIDAUTH_FTIME AS bidAuthFtime,\n"
                + "   trc.BIDAUTH_STATUS AS bidAuthStatus,\n"
                + "   trc.BIDAUTH_TIME AS bidAuthTime,\n"
                + "   trc.company_site AS companySite,\n"
                + "   trc.WWW_STATION AS wwwStation,\n"
                + "   trc.COMP_TYPE AS companyType,\n"
                + "   trcct.`NAME` AS companyTypStr,\n"
                + "   trc.FUND AS fund,\n"
                + "   trc.FUNDUNIT AS fundUnit,\n"
                + "   trc.INDUSTRY AS industryCode,\n"
                + "   trc.company_logo AS companyLogo,\n"
                + "   trc.MAIN_PRODUCT AS mainProduct,\n"
                + "   trc.WORKPATTERN AS workPattern,\n"
                + "   trc.TEL AS tel,\n"
                + "   trc.CONTACT AS contact,\n"
                + "   trc.CREATE_TIME AS createTime,\n"
                + "   trc.UPDATE_TIME AS updateTime,\n"
                + "   trc.WEB_TYPE AS webType,\n"
                + "   trc.status AS status,\n"
                + "   tru.LOGIN_NAME AS loginName,\n"
                + "   tru.`NAME` AS supplierName,\n"
                + "   tru.MOBILE AS mobile,\n"
                + "   tru.id AS userId,\n"
                + "\ttrc.core_status AS coreStatus \n"
                + "FROM\n"
                + "   t_reg_company trc\n"
                + "JOIN t_reg_user tru ON trc.id = tru.COMPANY_ID and trc.type = 13\n"
                + "LEFT JOIN t_reg_code_comp_type trcct ON trc.COMP_TYPE = trcct.ID\n"
                + "WHERE trc.UPDATE_TIME > ?\n"
                + "GROUP BY trc.ID\n"
                + "LIMIT ?,?";
        doSyncSupplierData(countUpdatedSql, queryUpdatedSql, lastSyncTime, SYNC_WAY_UPDATE);
    }

    private void doSyncSupplierData(String countSql, String querySql, Timestamp createTime, Integer syncWay) {
        List<Object> params = new ArrayList<>();
        params.add(createTime);
        long count = DBUtil.count(uniregDataSource, countSql, params);
        logger.info("执行countSql : {}, params : {}，共{}条", countSql, params, count);
        if (count > 0) {
            for (long i = 0; i < count; i += pageSize) {
                List<Object> paramsToUse = appendToParams(params, i);
                // 查出符合条件的供应商
                List<Map<String, Object>> resultToExecute = query(uniregDataSource, querySql, paramsToUse);
                logger.info("执行querySql : {}, params : {}，共{}条", querySql, paramsToUse, resultToExecute.size());
                Set<Long> supplierIds = new HashSet<>();
                for (Map<String, Object> result : resultToExecute) {
                    if (!StringUtils.isEmpty(result.get(AREA))) {
                        supplierIds.add((Long) result.get(ID));
                    }
                    refresh(result);
                }

                // 添加区域
                appendAreaStrToResult(resultToExecute, supplierIds);
                // 添加诚信等级 FIXME 诚信值表(目前默认为38)
                appendCreditToResult(resultToExecute, supplierIds);
                // 添加企业空间 FIXME 企业空间待更换数据源
                appendEnterpriseSpaceToResult(resultToExecute, supplierIds);
                // 插入数据保存,更新数据获取供应商交易信息
                appendSupplierTradingInfo(resultToExecute, supplierIds, syncWay);
            }
        }
    }

    private void appendSupplierTradingInfo(List<Map<String, Object>> resultToExecute, Set<Long> supplierIds, Integer syncWay) {
        if (Objects.equals(SYNC_WAY_UPDATE, syncWay)) {
            SearchResponse response = elasticClient.getTransportClient().prepareSearch(elasticClient.getProperties().getProperty("cluster.index"))
                    .setTypes(elasticClient.getProperties().getProperty("cluster.type.supplier"))
                    .setQuery(QueryBuilders.termsQuery("id", supplierIds))
                    .setSize(supplierIds.size())
                    .execute().actionGet();
            List<Map<String, Object>> resultFromEs = new ArrayList<>();
            for (SearchHit hit : response.getHits().getHits()) {
                resultFromEs.add(hit.getSource());
            }

            // 拷贝企业最新数据
            List<Map<String, Object>> mapList = resultFromEs.stream()
                    .map(esMap -> resultToExecute.stream()
                            .filter(map -> Objects.equals(map.get(ID), esMap.get(ID)))
                            .findFirst().map(map -> {
                                esMap.putAll(map);
                                return esMap;
                            }).orElse(null)
                    ).filter(Objects::nonNull).collect(Collectors.toList());
            batchExecute(mapList);
        } else {
            batchExecute(resultToExecute);
        }
    }

    private void refresh(Map<String, Object> resultToUse) {
        resultToUse.remove(AREA);
        // 添加不分词字段
        resultToUse.put(COMPANY_NAME_NOT_ANALYZED, resultToUse.get(COMPANY_NAME));
        resultToUse.put(MAIN_PRODUCT_NOT_ANALYZED, resultToUse.get(MAIN_PRODUCT));
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
        resultToUse.put(USER_ID, convertToString(resultToUse.get(USER_ID)));
        // 核心供状态
        Object coreStatus = resultToUse.get("coreStatus");
        resultToUse.put(CORE, coreStatus == null ? 0 :Integer.valueOf(String.valueOf(coreStatus).substring(0,1)));

        //添加平台来源
        resultToUse.put(BusinessConstant.PLATFORM_SOURCE_KEY,BusinessConstant.IXIETONG_SOURCE);
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


    /**
     * FIXME 诚信值默认为14
     *
     * @param resultToExecute
     * @param supplierIds
     */
    private void appendCreditToResult(List<Map<String, Object>> resultToExecute, Set<Long> supplierIds) {
        if (supplierIds.size() > 0) {
//            Map<Long, Object> creditMap = queryCredit(supplierIds);
//            for (Map<String, Object> result : resultToExecute) {
//                Long supplierId = Long.valueOf(String.valueOf(result.get(ID)));
//                Object creditRating = creditMap.get(supplierId);
//                // 如果没有诚信等级，就置为0
//                result.put(CREDIT_RATING, (creditRating == null ? 0 : creditRating));
//            }
            for (Map<String, Object> result : resultToExecute) {
                result.put(CREDIT_RATING, 14);
            }
        }
    }

//    private Map<Long, Object> queryCredit(Set<Long> supplierIds) {
//        String queryCreditTemplate = "SELECT COMPANY_ID as id, RATING as creditRating FROM credit_score WHERE COMPANY_ID in (%s)";
//        String queryCreditSql = String.format(queryCreditTemplate, StringUtils.collectionToCommaDelimitedString(supplierIds));
//        List<Map<String, Object>> query = query(creditDataSource, queryCreditSql, null);
//        Map<Long, Object> creditMap = new HashMap<>();
//        for (Map<String, Object> map : query) {
//            creditMap.put((Long) map.get(ID), map.get(CREDIT_RATING));
//        }
//        return creditMap;
//    }

    private void appendAreaStrToResult(List<Map<String, Object>> resultToExecute, Set<Long> supplierIds) {
        if (supplierIds.size() > 0) {
            Map<Long, AreaUtil.AreaInfo> areaInfoMap = AreaUtil.queryAreaInfo(uniregDataSource, supplierIds);
            for (Map<String, Object> result : resultToExecute) {
                Long supplierId = Long.valueOf(String.valueOf(result.get(ID)));
                AreaUtil.AreaInfo areaInfo = areaInfoMap.get(supplierId);
                if (areaInfo != null) {
                    result.put(AREA_STR, areaInfo.getAreaStr());
                    result.put(AREA_STR_NOT_ANALYZED, areaInfo.getAreaStr());
                    result.put(REGION, areaInfo.getRegion());
                    result.put(AREA_CODE, areaInfo.getAreaCode());
                }
            }
        }
    }

}
