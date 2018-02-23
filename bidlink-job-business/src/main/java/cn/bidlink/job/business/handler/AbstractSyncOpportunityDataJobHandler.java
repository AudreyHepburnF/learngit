package cn.bidlink.job.business.handler;

import cn.bidlink.job.common.es.ElasticClient;
import cn.bidlink.job.common.utils.DBUtil;
import cn.bidlink.job.common.utils.SyncTimeUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.ValueFilter;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.util.CollectionUtils;
import org.springframework.util.DigestUtils;
import org.springframework.util.StringUtils;

import javax.sql.DataSource;
import java.sql.Timestamp;
import java.util.*;

/**
 * @author : <a href="mailto:zikaifeng@ebnew.com">冯子恺</a>
 * @version : Ver 1.0
 * @description :
 * @date : 2017/8/25
 */
public abstract class AbstractSyncOpportunityDataJobHandler extends JobHandler {
    protected Logger logger = LoggerFactory.getLogger(this.getClass());

    // 每个省，直辖市对应的地区
    protected static final Map<String, String> regionMap;

    @Autowired
    protected ElasticClient elasticClient;

    @Autowired
    @Qualifier("ycDataSource")
    protected DataSource ycDataSource;

    @Autowired
    @Qualifier("centerDataSource")
    protected DataSource centerDataSource;

    // 有效的商机
    protected int VALID_OPPORTUNITY_STATUS   = 1;
    // 无效的商机
    protected int INVALID_OPPORTUNITY_STATUS = -1;
    // 招标项目类型
    protected int BIDDING_PROJECT_TYPE       = 1;
    // 采购项目类型
    protected int PURCHASE_PROJECT_TYPE      = 2;


    protected String ID                          = "id";
    protected String PURCHASE_ID                 = "purchaseId";
    protected String PROJECT_ID                  = "projectId";
    protected String PROJECT_TYPE                = "projectType";
    protected String DIRECTORY_NAME              = "directoryName";
    protected String DIRECTORY_NAME_NOT_ANALYZED = "directoryNameNotAnalyzed";
    protected String PURCHASE_NAME               = "purchaseName";
    protected String PROJECT_CODE                = "projectCode";
    protected String PROJECT_NAME                = "projectName";
    protected String PROJECT_NAME_NOT_ANALYZED   = "projectNameNotAnalyzed";
    protected String PROJECT_STATUS              = "projectStatus";
    protected String TENANT_KEY                  = "tenantKey";
    protected String AREA_STR                    = "areaStr";
    protected String REGION                      = "region";
    protected String STATUS                      = "status";
    protected String FIRST_DIRECTORY_NAME        = "firstDirectoryName";
    protected String DIRECTORY_NAME_COUNT        = "directoryNameCount";
    protected String QUOTE_STOP_TIME             = "quoteStopTime";
    protected String AREA                        = "area";
    protected String CODE                        = "code";
    protected String CITY                        = "city";
    protected String COUNTY                      = "county";


    protected Map<String, Object> appendIdToResult(Map<String, Object> result) {
        // 生成id
        result.put(ID, generateOpportunityId(result));
        return result;
    }

    private String generateOpportunityId(Map<String, Object> result) {
        Long projectId = (Long) result.get(PROJECT_ID);
        Long purchaseId = (Long) result.get(PURCHASE_ID);
        if (projectId == null) {
            throw new RuntimeException("商机ID生成失败，原因：项目ID为空!");
        }
        if (StringUtils.isEmpty(purchaseId)) {
            throw new RuntimeException("商机ID生成失败，原因：采购商ID为空!");
        }

        return DigestUtils.md5DigestAsHex((projectId + "_" + purchaseId).getBytes());
    }

    protected void doSyncProjectDataService(DataSource dataSource, String countSql, String querySql, List<Object> params) {
        long count = DBUtil.count(dataSource, countSql, params);
        logger.debug("执行countSql : {}, params : {}，共{}条", countSql, params, count);
        if (count > 0) {
            Timestamp currentDate = SyncTimeUtil.getCurrentDate();
            for (long i = 0; i < count; ) {
                List<Object> paramsToUse = appendToParams(params, i);
                // 查出符合条件的商机
                List<Map<String, Object>> results = DBUtil.query(dataSource, querySql, paramsToUse);
                logger.debug("执行querySql : {}, params : {}，共{}条", querySql, paramsToUse, results.size());
                List<Map<String, Object>> resultToExecute = new ArrayList<>();
                // 保存项目的采购品
                Map<Long, Set<String>> projectDirectoryMap = new LinkedHashMap<>();
                for (Map<String, Object> result : results) {
                    Long projectId = (Long) result.get(PROJECT_ID);
                    String directoryName = (String) result.get(DIRECTORY_NAME);
                    if (projectDirectoryMap.get(projectId) == null) {
                        projectDirectoryMap.put(projectId, new LinkedHashSet<String>());
                        parseOpportunity(currentDate, resultToExecute, result);
                    }
                    // 招标项目的采购品可能为空
                    if (!StringUtils.isEmpty(directoryName)) {
                        projectDirectoryMap.get(projectId).add(directoryName);
                    }
                }

                // 处理采购品
                Set<Long> purchaseIds = new HashSet<>();
                for (Map<String, Object> result : resultToExecute) {
                    purchaseIds.add((Long) result.get(PURCHASE_ID));
                    refresh(result, projectDirectoryMap);
                }

                // 添加tenantKey，以及区域
                appendTenantKeyAndAreaStrToResult(resultToExecute, purchaseIds);
                // 处理商机的状态
                batchExecute(resultToExecute);
                i += pageSize;
            }
        }
    }

    /**
     * 添加租户key tenantKey和区域 areaStr
     *
     * @param resultToExecute
     * @param purchaseIds
     */
    protected abstract void appendTenantKeyAndAreaStrToResult(List<Map<String, Object>> resultToExecute, Set<Long> purchaseIds);

    /**
     * 刷新字段
     *
     * @param result
     * @param projectDirectoryMap
     */
    protected void refresh(Map<String, Object> result, Map<Long, Set<String>> projectDirectoryMap) {
        Set<String> directoryNames = projectDirectoryMap.get(result.get(PROJECT_ID));
        if (!CollectionUtils.isEmpty(directoryNames)) {
            // 采购品
            String directoryNameList = StringUtils.collectionToCommaDelimitedString(directoryNames);
            result.put(DIRECTORY_NAME, directoryNameList);
            result.put(DIRECTORY_NAME_NOT_ANALYZED, directoryNameList);
            result.put(FIRST_DIRECTORY_NAME, directoryNames.iterator().next());
            result.put(DIRECTORY_NAME_COUNT, directoryNames.size());
        } else {
            result.put(DIRECTORY_NAME, null);
            result.put(DIRECTORY_NAME_NOT_ANALYZED, null);
            result.put(FIRST_DIRECTORY_NAME, null);
            result.put(DIRECTORY_NAME_COUNT, 0);
        }

        // 移除项目状态
        result.remove(PROJECT_STATUS);
        result.put(PROJECT_NAME_NOT_ANALYZED, result.get(PROJECT_NAME));
        // 同步时间
        result.put(SyncTimeUtil.SYNC_TIME, SyncTimeUtil.getCurrentDate());
        // 转换long字段类型，防止前端js精度溢出
        // for fuck js
        result.put(PROJECT_ID, String.valueOf(result.get(PROJECT_ID)));
        result.put(PURCHASE_ID, String.valueOf(result.get(PURCHASE_ID)));
    }

    ;

    /**
     * 解析商机数据
     *
     * @param currentDate
     * @param resultToExecute
     * @param result
     */
    protected abstract void parseOpportunity(Timestamp currentDate, List<Map<String, Object>> resultToExecute, Map<String, Object> result);

    /**
     * 查询采购商的tenantKey
     *
     * @param purchaseIds
     * @return
     */
    protected Map<Long, Object> queryTenantKey(Set<Long> purchaseIds) {
        String queryTenantKeySqlTemplate = "SELECT ID AS purchaseId,TENANT_ID AS tenantKey FROM t_reg_company WHERE TYPE = 12 AND TENANT_ID IS NOT NULL AND id in (%s)";
        String queryTenantKeySql = String.format(queryTenantKeySqlTemplate, StringUtils.collectionToCommaDelimitedString(purchaseIds));
        List<Map<String, Object>> query = DBUtil.query(centerDataSource, queryTenantKeySql, null);
        Map<Long, Object> tenantKeyMap = new HashMap<>();
        for (Map<String, Object> map : query) {
            tenantKeyMap.put((Long) map.get(PURCHASE_ID), map.get(TENANT_KEY));
        }
        return tenantKeyMap;
    }

    /**
     * 查询采购商的区域
     *
     * @param purchaseIds
     * @return
     */
    protected Map<Long, AreaInfo> queryArea(Set<Long> purchaseIds) {
        String queryAreaTemplate = "SELECT\n"
                                   + "   t3.ID AS purchaseId,\n"
                                   + "   t3.AREA AS area,\n"
                                   + "   t3.CODE AS code,\n"
                                   + "   t3.CITY AS city,\n"
                                   + "   tcrd.`VALUE` AS county\n"
                                   + "FROM\n"
                                   + "   (\n"
                                   + "      SELECT\n"
                                   + "         t2.ID, t2.AREA, t2.CODE,t2.COUNTY, tcrd.`VALUE` AS CITY\n"
                                   + "      FROM\n"
                                   + "         (\n"
                                   + "            SELECT\n"
                                   + "               t1.ID, t1.CITY, t1.COUNTY, tcrd.`KEY` AS CODE, tcrd.`VALUE` AS AREA\n"
                                   + "            FROM\n"
                                   + "               (SELECT ID, COUNTRY, AREA, CITY, COUNTY FROM t_reg_company WHERE ID IN (%s) AND TYPE = 12 AND COUNTRY IS NOT NULL) t1\n"
                                   + "            JOIN t_reg_center_dict tcrd ON t1.AREA = tcrd.`KEY`\n"
                                   + "            WHERE\n"
                                   + "               tcrd.TYPE = 'country'\n"
                                   + "         ) t2\n"
                                   + "      JOIN t_reg_center_dict tcrd ON t2.CITY = tcrd.`KEY`\n"
                                   + "      WHERE\n"
                                   + "         tcrd.TYPE = 'country'\n"
                                   + "   ) t3\n"
                                   + "JOIN t_reg_center_dict tcrd ON t3.COUNTY = tcrd.`KEY`\n"
                                   + "WHERE\n"
                                   + "   tcrd.TYPE = 'country'";

        String queryAreaSql = String.format(queryAreaTemplate, StringUtils.collectionToCommaDelimitedString(purchaseIds));
        List<Map<String, Object>> query = DBUtil.query(centerDataSource, queryAreaSql, null);
        Map<Long, AreaInfo> areaMap = new HashMap<>();
        for (Map<String, Object> map : query) {
            String areaStr = String.valueOf(map.get(AREA)) + String.valueOf(map.get(CITY)) + String.valueOf(map.get(COUNTY));
            // 特殊处理
            if (areaStr != null && areaStr.indexOf("市辖区") > -1) {
                areaStr = areaStr.replace("市辖区", "");
            }

            AreaInfo areaInfo = new AreaInfo(areaStr);
            // 处理省、直辖市
            String code = (String) map.get(CODE);
            if (code != null && code.length() > 2) {
                areaInfo.region = regionMap.get(code.substring(0, 2));
            }
            areaMap.put((Long) map.get(PURCHASE_ID), areaInfo);
        }
        return areaMap;
    }

    static class AreaInfo {
        private String areaStr;
        private String region;

        public String getAreaStr() {
            return areaStr;
        }

        public String getRegion() {
            return region;
        }

        public AreaInfo(String areaStr) {
            this.areaStr = areaStr;
        }

        public AreaInfo(String areaStr, String region) {
            this.areaStr = areaStr;
            this.region = region;
        }

        @Override
        public String toString() {
            return "RegionCla{" +
                   "areaStr='" + areaStr + '\'' +
                   ", region='" + region + '\'' +
                   '}';
        }
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
                                                      elasticClient.getProperties().getProperty("cluster.type.supplier_opportunity"),
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


    static {
        regionMap = new HashMap<>(64);
        /* 东北地区 */
        // 辽宁
        regionMap.put("21", "东北地区");
        // 吉林
        regionMap.put("22", "东北地区");
        // 黑龙江
        regionMap.put("23", "东北地区");
        /* 西北地区 */
        // 陕西
        regionMap.put("61", "西北地区");
        // 甘肃
        regionMap.put("62", "西北地区");
        // 青海
        regionMap.put("63", "西北地区");
        // 宁夏
        regionMap.put("64", "西北地区");
        // 新疆
        regionMap.put("65", "西北地区");
        /* 华北 */
        // 北京
        regionMap.put("11", "华北地区");
        // 天津
        regionMap.put("12", "华北地区");
        // 河北
        regionMap.put("13", "华北地区");
        // 山西
        regionMap.put("14", "华北地区");
        // 内蒙古
        regionMap.put("15", "华北地区");
        /* 华中 */
        // 江西
        regionMap.put("36", "华中地区");
        // 河南
        regionMap.put("41", "华中地区");
        // 湖北
        regionMap.put("42", "华中地区");
        // 湖南
        regionMap.put("43", "华中地区");
        /* 华东 */
        // 上海
        regionMap.put("31", "华东地区");
        // 江苏
        regionMap.put("32", "华东地区");
        // 浙江
        regionMap.put("33", "华东地区");
        // 安徽
        regionMap.put("34", "华东地区");
        // 福建
        regionMap.put("35", "华东地区");
        // 山东
        regionMap.put("37", "华东地区");
        /* 华南 */
        // 广东
        regionMap.put("44", "华南地区");
        // 广西
        regionMap.put("45", "华南地区");
        // 海南
        regionMap.put("46", "华南地区");
        /* 西南 */
        // 重庆
        regionMap.put("50", "西南地区");
        // 四川
        regionMap.put("51", "西南地区");
        // 贵州
        regionMap.put("52", "西南地区");
        // 云南
        regionMap.put("53", "西南地区");
        // 西藏
        regionMap.put("54", "西南地区");
        /* 特别行政区 */
        // 台湾
        regionMap.put("71", "特别行政区");
        // 香港
        regionMap.put("81", "特别行政区");
        // 澳门
        regionMap.put("82", "特别行政区");
    }
}

