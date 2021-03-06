package cn.bidlink.job.ycsearch.handler.projectexpress;

import cn.bidlink.job.common.constant.BusinessConstant;
import cn.bidlink.job.common.es.ElasticClient;
import cn.bidlink.job.common.utils.DBUtil;
import cn.bidlink.job.common.utils.SyncTimeUtil;
import cn.bidlink.job.ycsearch.handler.JobHandler;
import com.alibaba.fastjson.JSON;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.annotation.JobHander;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.DigestUtils;
import org.springframework.util.StringUtils;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;

import static cn.bidlink.job.common.utils.SyncTimeUtil.getZeroTime;
import static cn.bidlink.job.common.utils.SyncTimeUtil.getZeroTimeLongValue;

/**
 * @author <a href="mailto:zhihuizhou@ebnew.com">wisdom</a>
 * @version Ver 1.0
 * @description:项目直通车采购商项目推荐同步
 * @Date 2018/10/17
 */
@Service
@JobHander(value = "syncRecommendProjectDataJobHandler")
public class SyncRecommendProjectDataJobHandler extends JobHandler /*implements InitializingBean*/ {

    private Logger logger = LoggerFactory.getLogger(SyncRecommendProjectDataJobHandler.class);

    @Autowired
    private ElasticClient elasticClient;

    @Autowired
    @Qualifier("uniregDataSource")
    protected DataSource uniregDataSource;

    @Autowired
    @Qualifier("ycDataSource")
    protected DataSource ycDataSource;

    private String  PROJECT_TYPE          = "projectType";
    private String  UPDATE_TIME           = "updateTime";
    private Integer PURCHASE_PROJECT_TYPE = 2;
    private Integer CANAL_STATUS          = 10;
    private Integer ONLY_COOPERATE_JOIN   = 3;

    @Override
    public ReturnT<String> execute(String... strings) throws Exception {
        SyncTimeUtil.setCurrentDate();
        logger.info("0.开始同步项目直通车推荐项目数据");
        syncRecommendProjectData();
        logger.info("1.结束同步项目直通车推荐项目数据");
        return ReturnT.SUCCESS;
    }

    private void syncRecommendProjectData() {
        Properties properties = elasticClient.getProperties();
        SearchResponse response = elasticClient.getTransportClient().prepareSearch(properties.getProperty("cluster.supplier_opportunity_index"))
                .setTypes(properties.getProperty("cluster.type.supplier_opportunity"))
                .setQuery(QueryBuilders.boolQuery()
                        .must(QueryBuilders.termQuery(BusinessConstant.PLATFORM_SOURCE_KEY, BusinessConstant.YUECAI_SOURCE))
                        .must(QueryBuilders.termQuery(PROJECT_TYPE, PURCHASE_PROJECT_TYPE))
                        .must(QueryBuilders.rangeQuery(UPDATE_TIME).gte(SyncTimeUtil.toDateString(getZeroTime())))
                )
                .addSort("createTime", SortOrder.ASC)
                .setSize(pageSize)
                .setScroll(new TimeValue(60000))
                .execute().actionGet();
        do {
            SearchHits hits = response.getHits();
            for (SearchHit hit : hits.getHits()) {
                Map<String, Object> resultFromEs = hit.getSourceAsMap();
                Integer projectStatus = Integer.valueOf(resultFromEs.get("projectStatus").toString());
                Long projectId = Long.valueOf(resultFromEs.get("projectId").toString());
                Long purchaserId = Long.valueOf(resultFromEs.get("purchaseId").toString());
                String purchaserName = String.valueOf(resultFromEs.get("purchaseName"));
                String projectName = String.valueOf(resultFromEs.get("projectName"));
                logger.info("商机项目id:{},项目名称:{}", projectId, projectName);
                String projectCode = String.valueOf(resultFromEs.get("projectCode"));
                Integer coreSupplierProject = resultFromEs.get("isCore") == null ? 0 : Integer.valueOf(resultFromEs.get("isCore").toString());
                Integer status = Integer.valueOf(resultFromEs.get("status").toString());
                // TODO 待采购商机数据添加bidStopType字段
                Integer bidStopType = Integer.valueOf(resultFromEs.get("bidStopType").toString());
                Object bidStopTime = resultFromEs.get("quoteStopTime");
                Integer openRangeType=resultFromEs.get("openRangeType")==null?null:Integer.valueOf(resultFromEs.get("openRangeType").toString());
                //撤项消息
                if (CANAL_STATUS.equals(projectStatus)) {
                    logger.info("处理撤项商机数据:{}", resultFromEs);
                    List<Map<String, Object>> recommendProjects = this.getRecommendProjects(null, projectId, purchaserId);
                    if (!CollectionUtils.isEmpty(recommendProjects)) {
                        // 已撤项项目把匹配次数加回去
                        for (Map<String, Object> map : recommendProjects) {
                            Object orderCodeObj = map.get("orderCode");
                            if (orderCodeObj != null) {
                                returnMatchTimes(orderCodeObj.toString());
                            }
                        }
                        delMatchedProject(null, projectId, purchaserId);
                    }
                } else {//项目新建消息
                    Object directoryNameStr = resultFromEs.get("directoryName");
                    List<String> directoryNames = directoryNameStr == null ? Collections.emptyList() : Arrays.asList(directoryNameStr.toString().split(","));


                    Set<Long> supplierIds = new HashSet<Long>();
                    // 供应商和匹配到的采购商的对应关系
                    Map<String, Set<String>> supplierMatchedProductMap = new HashMap<String, Set<String>>();
                    // 商机推荐的供应商订单
                    List<Map<String, Object>> recommendSuppliers = new ArrayList<Map<String, Object>>();
                    // 拼接查询字符串
                    if (!CollectionUtils.isEmpty(directoryNames)) {
                        for (String directoryName : directoryNames) {
                            // 根据采购商机采购品匹配供应商项目直通车
                            List<Map<String, Object>> recommendSuppliersForOneDirectorys = this.getRecommendSuppliersOrders(directoryName, resultFromEs.get("updateTime").toString());
                            logger.info("根据采购品匹配到项目订单,recommendSuppliersForOneDirectorys:{}", JSON.toJSONString(recommendSuppliersForOneDirectorys));
                            if (!CollectionUtils.isEmpty(recommendSuppliersForOneDirectorys)) {
                                for (Map<String, Object> map : recommendSuppliersForOneDirectorys) {
                                    if (contains(directoryName, map)) {
                                        String supplierId = map.get("supplierId").toString();
                                        String orderCode = map.get("orderCode").toString();
                                        supplierIds.add(Long.valueOf(supplierId));
                                        String key = supplierId + "_" + orderCode;
                                        Set<String> products = supplierMatchedProductMap.get(key);
                                        if (products == null) {
                                            products = new HashSet<String>();
                                            products.add(directoryName);
                                            supplierMatchedProductMap.put(key, products);
                                        } else {
                                            products.add(directoryName);
                                        }
                                    }
                                }
                                recommendSuppliers.addAll(recommendSuppliersForOneDirectorys);
                            }

                        }
                    }

                    /**
                     * 去重订单
                     */
                    Map<String, Map<String, Object>> orderMap = new HashMap<String, Map<String, Object>>();
                    for (Map<String, Object> map : recommendSuppliers) {
                        String supplierId = map.get("supplierId").toString();
                        if (orderMap.containsKey(supplierId)) {
                            logger.info("订单去重:{}", map);
                            Map<String, Object> selectOrder = orderMap.get(supplierId);
                            /**
                             * 匹配最早的订单，通过创建时间来判断
                             */
                            Object orderCreateTimeObj = map.get("createTime");
                            Object selectOrderCreateTimeObj = selectOrder.get("createTime");
                            logger.info("orderCreateTimeObj：" + orderCreateTimeObj + "  selectOrderCreateTimeObj" + selectOrderCreateTimeObj);
                            if (orderCreateTimeObj != null && selectOrderCreateTimeObj != null) {
                                try {
                                    if (SyncTimeUtil.toStringDate(orderCreateTimeObj.toString()).getTime() < SyncTimeUtil.toStringDate(selectOrderCreateTimeObj.toString()).getTime()) {
                                        logger.info("匹配到更早订单：" + map.get("orderCode"));
                                        orderMap.put(supplierId, map);
                                    }
                                } catch (Exception e) {
                                    logger.error("时间转换错误：" + e.getMessage(), e);
                                }

                            }
                        } else {
                            logger.info(map.get("orderCode") + " 订单去重,未发现key：" + supplierId);
                            orderMap.put(supplierId, map);
                        }
                    }

                    /**
                     * 拉黑的供应商
                     */
                    List<Long> blackSupplierIds = getBlackSupplierIds(supplierIds, purchaserId);
                    List<Long> cooperateSupplierIds=new ArrayList<>();
                    if(ONLY_COOPERATE_JOIN.equals(openRangeType)){
                        cooperateSupplierIds=getCooperateSupplierIds(supplierIds,purchaserId);
                    }
                    Date now = new Date();

                    List<Map<String, Object>> recommendRecords = new ArrayList<Map<String, Object>>();
                    // 排除采购商拉黑的供应商订单
                    Collection<Map<String, Object>> recommendSupplierOrders = orderMap.values();
                    if (!CollectionUtils.isEmpty(recommendSupplierOrders) && !CollectionUtils.isEmpty(supplierMatchedProductMap)) {
                        for (Map<String, Object> map : recommendSupplierOrders) {
                            if (blackSupplierIds.contains(Long.valueOf(map.get("supplierId").toString()))) {
                                logger.info("供应商：[" + map.get("supplierName") + "] 已经被" + purchaserName + "拉黑");
                                continue;
                            }
                            if(ONLY_COOPERATE_JOIN.equals(openRangeType) && !cooperateSupplierIds.contains(Long.valueOf(map.get("supplierId").toString()))){
                                logger.info("供应商：[" + map.get("supplierName") + "] 不是" + purchaserName + "的合作供应商");
                                continue;
                            }
                            Set<String> products = supplierMatchedProductMap.get(map.get("supplierId").toString() + "_" + map.get("orderCode").toString());
                            StringBuilder matchedProducts = new StringBuilder("");
                            if (CollectionUtils.isEmpty(products)) {
                                logger.info("供应商id不匹配,跳过projectId:{}", projectId);
                                continue;
                            }
                            String recommendProjectId = DigestUtils.md5DigestAsHex((map.get("orderCode").toString() + "_" + projectId).getBytes());
                            logger.info("检查该项目是否推荐过该订单,是则项目直通车匹配次数不变,orderCode:{},projectId:{},recommendProjectId:{}", map.get("orderCode"), projectId, recommendProjectId);
                            boolean isDuplicationRecommend = this.checkRecommendProjectDuplication(recommendProjectId);
                            if (!isDuplicationRecommend) {
                                Object alreadyMatchTimes = map.get("alreadyMatchTimes");
                                map.put("alreadyMatchTimes", alreadyMatchTimes == null ? 1 : Integer.valueOf(alreadyMatchTimes.toString()) + 1);
                                map.put("latestMatchTime", now);
                                if (Long.valueOf(map.get("matchMark").toString()).longValue() < getZeroTimeLongValue()) {
                                    map.put("matchMark", getZeroTimeLongValue() + 1L);
                                } else {
                                    map.put("matchMark", Long.valueOf(map.get("matchMark").toString()) + 1L);
                                }
                                Object maxMatchTimes = map.get("productCode");
                                if (maxMatchTimes != null && alreadyMatchTimes != null
                                        && (Integer.valueOf(maxMatchTimes.toString()).intValue() <= Integer.valueOf(map.get("alreadyMatchTimes").toString()).intValue())) {
                                    map.put("orderEndDate", now);
                                    map.put("esOrderStatus", 0);
                                }
                            }

                            // 封装推荐项目数据
                            Map<String, Object> recommendRecord = new HashMap<String, Object>();
                            recommendRecord.put("projectId", String.valueOf(projectId));
                            recommendRecord.put("projectName", projectName);
                            recommendRecord.put("purchaserId", purchaserId);
                            recommendRecord.put("purchaserName", purchaserName);
                            // TODO 待修改
                            recommendRecord.put("bidStopType", bidStopType);
                            if (bidStopType == 2) {
                                // 自动截标添加截止时间
                                recommendRecord.put("bidStopTime", bidStopTime);
                            }
                            recommendRecord.put("supplierId", map.get("supplierId"));
                            recommendRecord.put("supplierName", map.get("supplierName"));
                            recommendRecord.put("projectCode", projectCode);
                            recommendRecord.put("orderCode", map.get("orderCode"));
                            recommendRecord.put("keywords", map.get("keywords"));
                            try {
                                Map<String, Object> supplierInfo = getSupplierInfo(Long.valueOf(map.get("supplierId").toString()));
                                recommendRecord.put("linkPhone", supplierInfo.get("linkPhone"));
                                recommendRecord.put("linkMan", supplierInfo.get("linkMan"));
                            } catch (Exception e) {
                                logger.error("供应商联系人，联系电话查询异常 供应商Id:{}", map.get("supplierId"));
                            }
                            for (String product : products) {
                                matchedProducts.append(product).append(";");
                            }
                            recommendRecord.put("projectStatus", status);
                            recommendRecord.put("matchedProducts", matchedProducts);
                            recommendRecord.put("coreSupplierProject", coreSupplierProject);
                            Timestamp currentDate = SyncTimeUtil.getCurrentDate();
                            recommendRecord.put(SyncTimeUtil.SYNC_TIME, currentDate);
                            recommendRecord.put("matchedDate", currentDate);
                            recommendRecord.put("createTime", currentDate);
                            recommendRecord.put("updateTime", null);
                            recommendRecord.put("attendStatus", 1);
                            recommendRecord.put("id", DigestUtils.md5DigestAsHex((map.get("orderCode").toString() + "_" + projectId).getBytes()));
                            recommendRecords.add(recommendRecord);
                        }
                        logger.info("保存项目直通车订单数据:{}", recommendSupplierOrders);
                        insertBatchToEs(recommendSupplierOrders, properties.getProperty("cluster.project_express_index"), properties.getProperty("cluster.type.project_express"));
                        logger.info("保存供应商项目直通车匹配到的项目信息:{}", recommendRecords);
                        insertBatchToEs(recommendRecords, properties.getProperty("cluster.project_express_supplier_recommend_record_index"), properties.getProperty("cluster.type.project_express_supplier_recommend_record"));
                    }
                }
            }
            response = elasticClient.getTransportClient().prepareSearchScroll(response.getScrollId())
                    .setScroll(new TimeValue(60000))
                    .execute().actionGet();
        } while (response.getHits().getHits().length != 0);
    }

    private Map<String, Object> getSupplierInfo(Long supplierId) {
        SearchResponse response = elasticClient.getTransportClient().prepareSearch("cluster.supplier_index")
                .setTypes("cluster.type.supplier")
                .setQuery(QueryBuilders.termQuery("id", String.valueOf(supplierId)))
                .execute().actionGet();
        SearchHits hits = response.getHits();
        Map<String, Object> map = new HashMap<>(2);
        if (hits.getTotalHits() > 0) {
            Map<String, Object> source = hits.getHits()[0].getSourceAsMap();
            map.put("linkMan", source.get("contact"));
            map.put("linkPhone", source.get("tel"));
        } else {
            map.put("linkMan", "");
            map.put("linkPhone", "");
        }
        return map;
    }

    /**
     * 检查该项目是否匹配过次订单 true:是重复匹配  false:不是
     *
     * @param recommendProjectId
     * @return
     */
    private boolean checkRecommendProjectDuplication(String recommendProjectId) {
        Properties properties = elasticClient.getProperties();
        SearchResponse response = elasticClient.getTransportClient().prepareSearch(properties.getProperty("cluster.project_express_supplier_recommend_record_index"))  //悦采索引
                .setTypes(properties.getProperty("cluster.type.project_express_supplier_recommend_record"))
                .setQuery(QueryBuilders.termQuery("id", recommendProjectId))
                .execute().actionGet();
        return response.getHits().getTotalHits() > 0 ? true : false;
    }

    /**
     * 查询采购商商机拉黑的供应商名单
     *
     * @param supplierIds
     * @param purchaserId
     * @return
     */
    private List<Long> getBlackSupplierIds(Set<Long> supplierIds, Long purchaserId) {
        if (!CollectionUtils.isEmpty(supplierIds)) {
            String querySqlTemplate = "SELECT DISTINCT\n" +
                    "\tsupplier_id \n" +
                    "FROM\n" +
                    "\tbsm_company_supplier \n" +
                    "WHERE\n" +
                    "\tcompany_id = ? \n" +
                    "\tAND supplier_status = 3  \n" +
                    "\tAND supplier_id IN (%s)";
            String querySql = String.format(querySqlTemplate, StringUtils.collectionToCommaDelimitedString(supplierIds));
            logger.info("查询匹配商机订单中被拉黑的供应商,querySql:{},params:{}", querySql, purchaserId);
            return DBUtil.query(ycDataSource, querySql, Collections.singletonList(purchaserId), new DBUtil.ResultSetCallback<List<Long>>() {
                @Override
                public List<Long> execute(ResultSet resultSet) throws SQLException {
                    ArrayList<Long> supplierIds = new ArrayList<>();
                    while (resultSet.next()) {
                        long supplierId = resultSet.getLong(1);
                        supplierIds.add(supplierId);
                    }
                    return supplierIds;
                }
            });
        } else {
            return Collections.emptyList();
        }
    }

    /**
     * 查询采购商合作的供应商名单
     *
     * @param supplierIds
     * @param purchaserId
     * @return
     */
    private List<Long> getCooperateSupplierIds(Set<Long> supplierIds, Long purchaserId) {
        if (!CollectionUtils.isEmpty(supplierIds)) {
            String querySqlTemplate = "SELECT DISTINCT\n" +
                    "\tsupplier_id \n" +
                    "FROM\n" +
                    "\tbsm_company_supplier \n" +
                    "WHERE\n" +
                    "\tcompany_id = ? \n" +
                    "\tAND supplier_status = 1  \n" +
                    "\tAND supplier_id IN (%s)";
            String querySql = String.format(querySqlTemplate, StringUtils.collectionToCommaDelimitedString(supplierIds));
            logger.info("查询匹配商机订单中合作供应商,querySql:{},params:{}", querySql, purchaserId);
            return DBUtil.query(ycDataSource, querySql, Collections.singletonList(purchaserId), new DBUtil.ResultSetCallback<List<Long>>() {
                @Override
                public List<Long> execute(ResultSet resultSet) throws SQLException {
                    ArrayList<Long> supplierIds = new ArrayList<>();
                    while (resultSet.next()) {
                        long supplierId = resultSet.getLong(1);
                        supplierIds.add(supplierId);
                    }
                    return supplierIds;
                }
            });
        } else {
            return Collections.emptyList();
        }
    }


    private void insertBatchToEs(Collection<Map<String, Object>> mapList, String index, String type) {
        if (!CollectionUtils.isEmpty(mapList)) {
            BulkRequestBuilder bulkRequest = elasticClient.getTransportClient().prepareBulk();
            for (Map<String, Object> map : mapList) {
                bulkRequest.add(elasticClient.getTransportClient().prepareIndex(index, type)
                        .setId(String.valueOf(map.get("id")))
                        .setSource(SyncTimeUtil.handlerDate(map))
                );
            }
//            bulkRequest.(true);
            BulkResponse response = bulkRequest.execute().actionGet();
            if (response.hasFailures()) {
                logger.error("保存项目直通车数据到es失败,错误信息:{}", response.buildFailureMessage());
            }
        }
    }

    /**
     * shiyongren
     *
     * @param supplierId
     * @param projectId
     * @param purchaserId
     * @return
     * @author : <a href="mailto:shiyongren@ebnew.com">shiyongren</a>  2017年5月16日 下午2:04:31
     */
    private List<Map<String, Object>> getRecommendProjects(Long supplierId, Long projectId, Long purchaserId) {

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        if (supplierId != null) {
            boolQueryBuilder.must(QueryBuilders.termQuery("supplierId", supplierId));
        }
        if (projectId != null) {
            boolQueryBuilder.must(QueryBuilders.termQuery("projectId", projectId));
        }
        if (purchaserId != null) {
            boolQueryBuilder.must(QueryBuilders.termQuery("purchaserId", purchaserId));
        }
        Properties properties = elasticClient.getProperties();
        SearchResponse response = elasticClient.getTransportClient().prepareSearch(properties.getProperty("cluster.project_express_supplier_recommend_record_index"))  //悦采索引
                .setTypes(properties.getProperty("cluster.type.project_express_supplier_recommend_record"))
                .setQuery(boolQueryBuilder) //设置查询条件
                .setFrom(0).setSize(10000) //设置分页属性
                .execute().actionGet();


        SearchHits hits = response.getHits();  //获取结果

        List<Map<String, Object>> recommendProjects = new ArrayList<Map<String, Object>>();

        long total = hits.getTotalHits();  // 记录总数

        if (total == 0) {
            return recommendProjects;
        }

        for (SearchHit hit : hits.getHits()) {
            Map<String, Object> res = hit.getSourceAsMap();
            recommendProjects.add(res);
        }
        return recommendProjects;
    }

    /**
     * shiyongren 撤销项目后，被改项目匹配到的供应商匹配次数-1，返回匹配次数
     *
     * @param orderCode
     * @author : <a href="mailto:shiyongren@ebnew.com">shiyongren</a>  2017年5月27日 上午11:09:55
     */
    private void returnMatchTimes(String orderCode) {
        QueryBuilder orderCodeBuilder = QueryBuilders.termQuery("orderCode", orderCode);
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(orderCodeBuilder);
        Properties properties = elasticClient.getProperties();
        SearchResponse response = elasticClient.getTransportClient().prepareSearch(properties.getProperty("cluster.project_express_index"))  //悦采索引
                .setTypes(properties.getProperty("cluster.type.project_express"))
                .setQuery(boolQueryBuilder) //设置查询条件
                .setFrom(0).setSize(10000) //设置分页属性
                .execute().actionGet();

        SearchHits hits = response.getHits();  //获取结果

        List<Map<String, Object>> bsProjectExpresses = new ArrayList<Map<String, Object>>();

        long total = hits.getTotalHits();  // 记录总数

        if (total == 0) {
            return;
        }


        Date now = new Date();

        for (SearchHit hit : hits.getHits()) {
            Map<String, Object> res = hit.getSourceAsMap();
            Object matchTimesObj = res.get("productCode");
            Object alreadyMatchTimesObj = res.get("alreadyMatchTimes");
            if (matchTimesObj == null || alreadyMatchTimesObj == null) {
                continue;
            }
            res.put("alreadyMatchTimes", Integer.valueOf(res.get("alreadyMatchTimes").toString()) - 1); //
            res.put("matchMark", Long.valueOf(res.get("matchMark").toString()) - 1L); //
            if ((Integer.valueOf(matchTimesObj.toString()).intValue() > Integer.valueOf(res.get("alreadyMatchTimes").toString()).intValue())) {
                res.put("orderEndDate", null);
                res.put("esOrderStatus", 1);
                res.put("updateTime", now);
            }
            bsProjectExpresses.add(res);
        }

        insertBatchToEs(bsProjectExpresses, properties.getProperty("cluster.project_express_index"), properties.getProperty("cluster.type.project_express"));

    }

    /**
     * shiyongren 删除匹配的项目
     *
     * @author : <a href="mailto:shiyongren@ebnew.com">shiyongren</a>  2017年6月9日 下午5:08:17
     */
    private void delMatchedProject(Long supplierId, Long projectId, Long purchaserId) {
        try {
            //删除ES中的老数据
            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
            if (supplierId != null) {
                boolQueryBuilder.must(QueryBuilders.termQuery("supplierId", supplierId));
            }
            if (purchaserId != null) {
                boolQueryBuilder.must(QueryBuilders.termQuery("projectId", projectId));
            }
            if (projectId != null) {
                boolQueryBuilder.must(QueryBuilders.termQuery("purchaserId", purchaserId));
            }
            Properties properties = elasticClient.getProperties();
            BulkByScrollResponse scrollResponse = DeleteByQueryAction.INSTANCE.newRequestBuilder(elasticClient.getTransportClient())
                    .filter(boolQueryBuilder)
                    .source(properties.getProperty("cluster.express_index"))
                    .get();
            if (CollectionUtils.isEmpty(scrollResponse.getBulkFailures())) {
                logger.error("删除es中匹配的老数据失败");
            }

//            DeleteByQueryRequestBuilder builder = new DeleteByQueryRequestBuilder(elasticClient.getTransportClient(), DeleteByQueryAction.INSTANCE)
//                    .setIndices(properties.getProperty("cluster.express_index")).setTypes(properties.getProperty("cluster.type.project_express_supplier_recommend_record"))
//                    .setQuery(boolQueryBuilder);
//
//            builder.execute().actionGet();

//            // 更新其中一条数据
//            SearchResponse response = elasticClient.getTransportClient().prepareSearch(properties.getProperty("cluster.express_index")).setTypes(properties.getProperty("cluster.type.project_express_supplier_recommend_record"))
//                    .setSize(1).execute().actionGet();
//            SearchHits hits = response.getHits();
//            if (hits.getTotalHits() > 0) {
//                Map<String, Object> source = hits.getHits()[0].getSourceAsMap();
//                source.put(SYNC_TIME, SyncTimeUtil.currentDateToString());
//                elasticClient.getTransportClient().prepareUpdate(properties.getProperty("cluster.express_index"), properties.getProperty("cluster.type.project_express_supplier_recommend_record"), String.valueOf(source.get("id")))
//                        .setDoc(source)
//                        .execute().actionGet();
        } catch (Exception e) {
            logger.error("项目直通车数据从ElasticSearch删除失败: " + e.getMessage(), e);
        }

    }


    /**
     * shiyongren 查询需要推荐的供应商订单信息
     *
     * @param queryStr
     * @param updateTime
     * @return
     * @author : <a href="mailto:shiyongren@ebnew.com">shiyongren</a>  2017年5月10日 下午3:02:22
     */
    private List<Map<String, Object>> getRecommendSuppliersOrders(String queryStr, String updateTime) {
        //支付状态10 支付成功
        QueryBuilder paidStatusBuilder = QueryBuilders.termQuery("paidStatus", 10);
        //订单状态5 订单生效
        QueryBuilder orderStatusBuilder = QueryBuilders.termQuery("orderStatus", 5);
//		//es标记的订单状态为1：表示还有可用匹配次数
        QueryBuilder esOrderStatusBuilder = QueryBuilders.termQuery("esOrderStatus", 1);
        //查询关键字
        QueryBuilder keyWordsBuilder = QueryBuilders.matchQuery("keywords", queryStr);

        //每天限制2次匹配
        QueryBuilder matchMarkBuilder = QueryBuilders.rangeQuery("matchMark").lt(getZeroTimeLongValue() + 2L);

        // 推荐项目需要在订单创建之后
        RangeQueryBuilder updateTimeBuilder = QueryBuilders.rangeQuery("createTime").lt(updateTime);
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(paidStatusBuilder)
                .must(orderStatusBuilder)
                .must(esOrderStatusBuilder)
                .must(matchMarkBuilder)
                .must(updateTimeBuilder)
                .must(keyWordsBuilder);

        Properties properties = elasticClient.getProperties();
        SearchResponse response = elasticClient.getTransportClient().prepareSearch(properties.getProperty("cluster.project_express_index"))  //悦采索引
                .setTypes(properties.getProperty("cluster.type.project_express"))
                .setQuery(boolQueryBuilder) //设置查询条件
                .setFrom(0).setSize(10000) //设置分页属性
                .execute().actionGet();


        SearchHits hits = response.getHits();  //获取结果

        List<Map<String, Object>> bsProjectExpresses = new ArrayList<Map<String, Object>>();

        long total = hits.getTotalHits();  // 记录总数

        if (total == 0) {
            return bsProjectExpresses;
        }


        for (SearchHit hit : hits.getHits()) {
            Map<String, Object> res = hit.getSourceAsMap();
            bsProjectExpresses.add(res);
        }
        return bsProjectExpresses;
    }

    /**
     * 如果directoryName包含关键字
     *
     * @param directoryName
     * @param map
     * @return
     */
    private boolean contains(String directoryName, Map<String, Object> map) {
        String keywords = map.get("keywords").toString();
        String[] keywordList = keywords.replaceAll("[、,。.，；;/\\t\\s]", ",").split(",");
        for (String keyword : keywordList) {
            if (!StringUtils.isEmpty(keyword) && directoryName.indexOf(keyword) > -1) {
                return true;
            }
        }
        return false;
    }

    /*@Override
    public void afterPropertiesSet() throws Exception {
        execute();
    }*/
}
