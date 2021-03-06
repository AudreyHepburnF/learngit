package cn.bidlink.job.ycsearch.handler;

import cn.bidlink.job.common.es.ElasticClient;
import cn.bidlink.job.common.utils.AreaUtil;
import cn.bidlink.job.common.utils.DBUtil;
import cn.bidlink.job.common.utils.SyncTimeUtil;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.util.CollectionUtils;
import org.springframework.util.DigestUtils;
import org.springframework.util.StringUtils;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;

import static cn.bidlink.job.common.utils.AreaUtil.queryAreaInfo;

/**
 * @author <a href="mailto:zhihuizhou@ebnew.com">zhouzhihui</a>
 * @version Ver 1.0
 * @description:
 * @Date 2018/9/5
 */
public abstract class AbstractSyncYcOpportunityDataJobHandler extends JobHandler {

    protected Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    protected ElasticClient elasticClient;

    @Autowired
    @Qualifier("uniregDataSource")
    protected DataSource uniregDataSource;

    @Autowired
    @Qualifier("ycDataSource")
    protected DataSource ycDataSource;
    // 有效的商机
    protected int    VALID_OPPORTUNITY_STATUS   = 1;
    // 无效的商机
    protected int    INVALID_OPPORTUNITY_STATUS = -1;
    // 招标项目类型
    protected int    BIDDING_PROJECT_TYPE       = 1;
    // 采购项目类型
    protected int    PURCHASE_PROJECT_TYPE      = 2;
    // 竞价项目类型
    protected int    AUCTION_PROJECT_TYPE       = 3;
    // 招募类型
    protected int    RECRUIT_PROJECT_TYPE       = 4;
    // 新平台数据
    protected String SOURCE_NEW                 = "new";
    // 老平台数据
    protected String SOURCE_OLD                 = "old";

    // 是否展示
    protected String IS_SHOW = "isShow";

    protected int SHOW   = 1;
    protected int HIDDEN = 0;


    protected String ID                          = "id";
    protected String PURCHASE_ID                 = "purchaseId";
    protected String PROJECT_ID                  = "projectId";
    protected String SOURCE_ID                   = "sourceId";
    protected String PROJECT_TYPE                = "projectType";
    protected String DIRECTORY_NAME              = "directoryName";
    protected String DIRECTORY_ID                = "directoryId";
    protected String DIRECTORY_NAME_NOT_ANALYZED = "directoryNameNotAnalyzed";
    protected String PURCHASE_NAME               = "purchaseName";
    protected String PURCHASE_NAME_NOT_ANALYZED  = "purchaseNameNotAnalyzed";
    protected String PROJECT_CODE                = "projectCode";
    protected String PROJECT_NAME                = "projectName";
    protected String PROJECT_NAME_NOT_ANALYZED   = "projectNameNotAnalyzed";
    protected String PROJECT_STATUS              = "projectStatus";
    protected String AREA_STR                    = "areaStr";
    protected String AREA_STR_NOT_ANALYZED       = "areaStrNotAnalyzed";
    protected String REGION                      = "region";
    protected String STATUS                      = "status";
    protected String FIRST_DIRECTORY_NAME        = "firstDirectoryName";
    protected String DIRECTORY_NAME_COUNT        = "directoryNameCount";
    protected String QUOTE_STOP_TIME             = "quoteStopTime";
    protected String PROCESS_STATUS              = "processStatus";
    protected String AREA                        = "area";
    protected String CODE                        = "code";
    protected String CITY                        = "city";
    protected String COUNTY                      = "county";
    protected String PROVINCE                    = "province";
    // 数据来源，new表示新平台，old表示老平台
    protected String SOURCE                      = "source";


    protected Map<String, Object> appendIdToResult(Map<String, Object> result) {
        // 生成id
        result.put(ID, generateOpportunityId(result));
        return result;
    }

    /**
     * id生成器
     * 注意：如果商机id生成策略变更了，需要重新导入一次数据
     *
     * @param result
     * @return
     */
    protected String generateOpportunityId(Map<String, Object> result) {
        Long projectId = (Long) result.get(PROJECT_ID);
        Long purchaseId = (Long) result.get(PURCHASE_ID);
        if (projectId == null) {
            throw new RuntimeException("商机ID生成失败，原因：项目ID为空!");
        }
        if (StringUtils.isEmpty(purchaseId)) {
            throw new RuntimeException("商机ID生成失败，原因：采购商ID为空!");
        }

        return DigestUtils.md5DigestAsHex((projectId + "_" + purchaseId + "_" + SOURCE_OLD).getBytes());
    }


    protected class DirectoryEntity {
        protected Long   directoryId;
        protected String directoryName;

        public DirectoryEntity(Long directoryId, String directoryName) {
            this.directoryId = directoryId;
            this.directoryName = directoryName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            DirectoryEntity that = (DirectoryEntity) o;

            if (directoryId != null ? !directoryId.equals(that.directoryId) : that.directoryId != null) return false;
            return directoryName != null ? directoryName.equals(that.directoryName) : that.directoryName == null;

        }

        @Override
        public int hashCode() {
            int result = directoryId != null ? directoryId.hashCode() : 0;
            result = 31 * result + (directoryName != null ? directoryName.hashCode() : 0);
            return result;
        }
    }

    protected void doSyncProjectDataService(DataSource dataSource, String countSql, String querySql, List<Object> params) {
        long count = DBUtil.count(dataSource, countSql, params);
        logger.info("执行countSql : {}, params : {}，共{}条", countSql, params, count);
        if (count > 0) {
            Timestamp currentDate = SyncTimeUtil.getCurrentDate();
            for (long i = 0; i < count; ) {
                List<Object> paramsToUse = appendToParams(params, i);
                // 查出符合条件的商机
                List<Map<String, Object>> results = DBUtil.query(dataSource, querySql, paramsToUse);
                logger.info("执行querySql : {}, params : {}，共{}条", querySql, paramsToUse, results.size());
                List<Map<String, Object>> resultToExecute = new ArrayList<>();
                // 保存项目的采购品
                Map<Long, Set<DirectoryEntity>> projectDirectoryMap = new LinkedHashMap<>();
                for (Map<String, Object> result : results) {
                    Long projectId = (Long) result.get(PROJECT_ID);
                    String directoryName = (String) result.get(DIRECTORY_NAME);
                    Long directoryId = (Long) result.get(DIRECTORY_ID);

                    if (projectDirectoryMap.get(projectId) == null) {
                        projectDirectoryMap.put(projectId, new LinkedHashSet<DirectoryEntity>());
                        parseOpportunity(currentDate, resultToExecute, result);
                    }
                    // 招标项目的采购品可能为空
                    if (!StringUtils.isEmpty(directoryName)) {
                        projectDirectoryMap.get(projectId).add(new DirectoryEntity(directoryId, directoryName));
                    }
                }

                // 处理采购商id
                Set<Long> purchaseIds = new HashSet<>();
                for (Map<String, Object> result : resultToExecute) {
                    purchaseIds.add((Long) result.get(PURCHASE_ID));
                }

                // 添加区域
                appendAreaStrToResult(resultToExecute, purchaseIds);

                for (Map<String, Object> result : resultToExecute) {
                    purchaseIds.add((Long) result.get(PURCHASE_ID));
                    refresh(result, projectDirectoryMap);
                }
                if (this instanceof SyncBidTypeOpportunityToXtDataJobHandler) {
                    this.appendCompanyTenantSite(resultToExecute);
                }
                // 处理商机的状态
                batchExecute(resultToExecute);

                i += pageSize;
            }
        }
    }

//    /**
//     * 项目重建删除项目和公告
//     *
//     * @param resultToExecute
//     */
//    protected void rebuildProjectDelete(List<Map<String, Object>> resultToExecute) {
//        if (!CollectionUtils.isEmpty(resultToExecute)) {
//            Set<Object> rebuildProjectIds = resultToExecute.stream()
//                    .filter(map -> Integer.valueOf(map.get(PROJECT_STATUS).toString()) < 5).map(map -> map.get(PROJECT_ID)).collect(Collectors.toSet());
//            deleteProjectAndNotice(rebuildProjectIds);
//        }
//    }
//
//    private void deleteProjectAndNotice(Set<Object> rebuildProjectIds) {
//        if (!CollectionUtils.isEmpty(rebuildProjectIds)) {
//            logger.info("1.项目重建项目信息:{}", JSON.toJSONString(rebuildProjectIds));
//            Properties properties = elasticClient.getProperties();
//            BoolQueryBuilder query = QueryBuilders.boolQuery();
//            query.must(QueryBuilders.termQuery(BusinessConstant.PLATFORM_SOURCE_KEY, BusinessConstant.YUECAI_SOURCE));
//            query.must(QueryBuilders.termQuery(PROJECT_TYPE, PURCHASE_PROJECT_TYPE));
//            rebuildProjectIds.forEach(rebuildProjectId -> query.should(QueryBuilders.termQuery(PROJECT_ID, rebuildProjectId)));
//            // 删除商机
//            DeleteByQueryRequestBuilder deleteRequest = new DeleteByQueryRequestBuilder(elasticClient.getTransportClient(), DeleteByQueryAction.INSTANCE)
//                    .setIndices(properties.getProperty("cluster.index")).setTypes(properties.getProperty("cluster.type.supplier_opportunity"))
//                    .setQuery(query);
//            deleteRequest.execute().actionGet();
//
//            // 删除公告
//            DeleteByQueryRequestBuilder noticeDeleteRequest = new DeleteByQueryRequestBuilder(elasticClient.getTransportClient(), DeleteByQueryAction.INSTANCE)
//                    .setIndices(properties.getProperty("cluster.index")).setTypes(properties.getProperty("cluster.type.notice"))
//                    .setQuery(query);
//            noticeDeleteRequest.execute().actionGet();
//        }
//    }

    private void appendCompanyTenantSite(List<Map<String, Object>> resultToExecute) {
        Set<Long> companyIds = resultToExecute.stream().map(map -> Long.valueOf(map.get("purchaseId").toString())).collect(Collectors.toSet());
        String querySqlTemplate = "SELECT\n" +
                "\tid AS companyId,\n" +
                "\tTENANT_SITE AS tenantSite \n" +
                "FROM\n" +
                "\tt_reg_company \n" +
                "WHERE\n" +
                "\tid IN (%s)";
        String querySql = String.format(querySqlTemplate, StringUtils.collectionToCommaDelimitedString(companyIds));
        Map<Long, Object> tenantSiteMap = DBUtil.query(uniregDataSource, querySql, null, new DBUtil.ResultSetCallback<Map<Long, Object>>() {
            @Override
            public Map<Long, Object> execute(ResultSet resultSet) throws SQLException {
                Map<Long, Object> map = new HashMap<>();
                while (resultSet.next()) {
                    map.put(resultSet.getLong(1), resultSet.getString(2));
                }
                return map;
            }
        });
        resultToExecute.forEach(map -> map.put("tenantSite", tenantSiteMap.get(Long.valueOf(map.get("purchaseId").toString()))));
    }

    /**
     * 添加区域 areaStr
     *
     * @param resultToExecute
     * @param purchaseIds
     */
    protected void appendAreaStrToResult(List<Map<String, Object>> resultToExecute, Set<Long> purchaseIds) {
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
                    if (Objects.isNull(result.get(PURCHASE_NAME))) {
                        // 如果项目采购商公司名称为空 取中心库
                        result.put(PURCHASE_NAME, areaInfo.getPurchaseName());
                    }
                }
            }
        }
    }

    /**
     * 刷新字段
     *
     * @param result
     * @param projectDirectoryMap
     */
    protected void refresh(Map<String, Object> result, Map<Long, Set<DirectoryEntity>> projectDirectoryMap) {
        Set<DirectoryEntity> directoryEntities = projectDirectoryMap.get(result.get(PROJECT_ID));
        if (!CollectionUtils.isEmpty(directoryEntities)) {
            // 采购品
            String directoryNameList = getDirectoryNames(directoryEntities);
            result.put(DIRECTORY_NAME, directoryNameList);
            result.put(DIRECTORY_NAME_NOT_ANALYZED, directoryNameList);
            result.put(FIRST_DIRECTORY_NAME, directoryEntities.iterator().next().directoryName);
            result.put(DIRECTORY_NAME_COUNT, directoryEntities.size());
        } else {
            result.put(DIRECTORY_NAME, null);
            result.put(DIRECTORY_NAME_NOT_ANALYZED, null);
            result.put(FIRST_DIRECTORY_NAME, null);
            result.put(DIRECTORY_NAME_COUNT, 0);
        }
        // 添加不分词字段
        result.put(PROJECT_NAME_NOT_ANALYZED, result.get(PROJECT_NAME));
        result.put(PURCHASE_NAME_NOT_ANALYZED, result.get(PURCHASE_NAME));

        // 同步时间
        result.put(SyncTimeUtil.SYNC_TIME, SyncTimeUtil.getCurrentDate());
        // 转换long字段类型，防止前端js精度溢出
        // for fuck js
        if (result.get(DIRECTORY_ID) != null) {
            result.put(DIRECTORY_ID, String.valueOf(result.get(DIRECTORY_ID)));
        }
        result.put(PROJECT_ID, String.valueOf(result.get(PROJECT_ID)));
        result.put(PURCHASE_ID, String.valueOf(result.get(PURCHASE_ID)));
        result.put(SOURCE_ID, String.valueOf(result.get(SOURCE_ID)));
        result.put(AREA_STR_NOT_ANALYZED, result.get(AREA_STR));
    }

    /**
     * 获取所有的采购品名称
     *
     * @param directoryEntities
     * @return
     */
    private String getDirectoryNames(Set<DirectoryEntity> directoryEntities) {
        List<String> directoryNames = new ArrayList<>();
        for (DirectoryEntity directoryEntity : directoryEntities) {
            directoryNames.add(directoryEntity.directoryName);
        }
        return StringUtils.collectionToCommaDelimitedString(directoryNames);
    }


    /**
     * 解析商机数据
     *
     * @param currentDate
     * @param resultToExecute
     * @param result
     */
    protected abstract void parseOpportunity(Timestamp currentDate, List<Map<String, Object>> resultToExecute, Map<String, Object> result);

    protected void batchExecute(List<Map<String, Object>> resultsToUpdate) {
//        logger.info("插入数据的结果:{}", JSON.toJSONString(resultsToUpdate));
        if (!CollectionUtils.isEmpty(resultsToUpdate)) {
            BulkRequestBuilder bulkRequest = elasticClient.getTransportClient().prepareBulk();
            for (Map<String, Object> result : resultsToUpdate) {
                bulkRequest.add(elasticClient.getTransportClient()
                        .prepareUpdate(elasticClient.getProperties().getProperty("cluster.supplier_opportunity_index"),
                                elasticClient.getProperties().getProperty("cluster.type.supplier_opportunity"),
                                String.valueOf(result.get(ID)))
                        .setDocAsUpsert(true)
                        .setDoc(SyncTimeUtil.handlerDate(result)));
            }

            bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            BulkResponse response = bulkRequest.execute().actionGet();
            if (response.hasFailures()) {
                logger.error(response.buildFailureMessage());
            }
        }
    }
}
