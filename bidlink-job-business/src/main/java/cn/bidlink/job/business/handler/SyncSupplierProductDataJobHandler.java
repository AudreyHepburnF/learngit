package cn.bidlink.job.business.handler;

import cn.bidlink.job.common.es.ElasticClient;
import cn.bidlink.job.common.utils.DBUtil;
import cn.bidlink.job.common.utils.ElasticClientUtil;
import cn.bidlink.job.common.utils.SyncTimeUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.ValueFilter;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.IJobHandler;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.DigestUtils;
import org.springframework.util.StringUtils;

import javax.sql.DataSource;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 同步供应商产品数据
 *
 * @author : <a href="mailto:zikaifeng@ebnew.com">冯子恺</a>
 * @version : Ver 1.0
 * @description :
 * @date : 2017/8/7
 */
@JobHander(value = "syncProductDataJobHandler")
@Service
public class SyncSupplierProductDataJobHandler extends IJobHandler /*implements InitializingBean*/ {
    private Logger logger = LoggerFactory.getLogger(SyncSupplierProductDataJobHandler.class);

    @Autowired
    private ElasticClient elasticClient;

    @Autowired
    @Qualifier("proDataSource")
    private DataSource proDataSource;

    @Autowired
    @Qualifier("ycDataSource")
    private DataSource ycDataSource;

    @Autowired
    @Qualifier("centerDataSource")
    private DataSource centerDataSource;

    @Value("${pageSize:200}")
    private int pageSize;

    private String ID                     = "id";
    private String DIRECTORY_NAME         = "directoryNameAlias";
    private String SUPPLIER_ID            = "supplierId";
    private String SUPPLIER_DIRECTORY_REL = "supplierDirectoryRel";

    private int             threadNum       = 10;
    private Semaphore       semaphore       = new Semaphore(threadNum);
    private AtomicLong      atomicLong      = new AtomicLong(0);
    private ExecutorService executorService = Executors.newFixedThreadPool(threadNum, new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, "syncSupplierProduct-thread-" + atomicLong.getAndIncrement());
            t.setDaemon(true);
            return t;
        }
    });

    public ReturnT<String> execute(String... strings) throws Exception {
        SyncTimeUtil.setCurrentDate();
        logger.info("供应商产品数据同步开始");
        syncProductData();
        logger.info("供应商产品数据同步结束");
        return ReturnT.SUCCESS;
    }

    // 供应商产品关系（1、报价产品；2、中标产品；3、标王关键词；4、主营产品：）
    private void syncProductData() {
        Timestamp lastSyncTime = ElasticClientUtil.getMaxTimestamp(elasticClient, "cluster.index", "cluster.type.supplier_product", null);
        syncTradeProductDataService(lastSyncTime);
        syncTradeBidProductDataService(lastSyncTime);
        syncProDataService(lastSyncTime);
        syncCenterDataService(lastSyncTime);
    }


    /**
     * 同步交易过的采购品
     * 注意：采购品中标状态只能更新为中标状态，所以只通过updateTime来查询数据
     *
     * @param lastSyncTime
     */
    private void syncTradeProductDataService(Timestamp lastSyncTime) {
        logger.info("同步交易过的采购品开始");
        String countUpdatedSql = "SELECT\n"
                                 + "   count(1)\n"
                                 + "FROM\n"
                                 + "   bmpfjz_supplier_project_bid bspb\n"
                                 + "JOIN bmpfjz_supplier_project_item_bid bspib ON bspb.id = bspib.supplier_project_bid_id\n"
                                 + "JOIN corp_directorys cd ON bspib.directory_id = cd.ID\n"
                                 + "WHERE\n"
                                 + "   (bspb.supplier_bid_status = 2\n"
                                 + "OR bspb.supplier_bid_status = 3)\n"
                                 + "   AND bspb.update_time > ?";
        String queryUpdatedSql = "SELECT\n"
                                 + "   bspb.SUPPLIER_ID AS supplierId,\n"
                                 + "   bspb.SUPPLIER_NAME AS supplierName,\n"
                                 + "   cd.`NAME` AS directoryNameAlias,\n"
                                 + "   1 AS supplierDirectoryRel,\n"
                                 + "   bspb.CREATE_TIME AS createTime\n"
                                 + "FROM\n"
                                 + "   bmpfjz_supplier_project_bid bspb\n"
                                 + "JOIN bmpfjz_supplier_project_item_bid bspib ON bspb.id = bspib.supplier_project_bid_id\n"
                                 + "JOIN corp_directorys cd ON bspib.directory_id = cd.ID\n"
                                 + "WHERE\n"
                                 + "   (bspb.supplier_bid_status = 2\n"
                                 + "OR bspb.supplier_bid_status = 3)\n"
                                 + "   AND bspb.update_time > ?\n"
                                 + "LIMIT ?, ?";
        doSyncUpdatedData(ycDataSource, countUpdatedSql, queryUpdatedSql, lastSyncTime);
        logger.info("同步交易过的采购品结束");
    }

    /**
     * 同步中标的产品
     *
     * @param lastSyncTime
     */
    private void syncTradeBidProductDataService(Timestamp lastSyncTime) {
        logger.info("同步中标的产品开始");
        String countUpdatedSql = "SELECT\n"
                                 + "   count(1)\n"
                                 + "FROM\n"
                                 + "   bid b\n"
                                 + "JOIN bid_product bp ON b.ID = bp.BID_ID\n"
                                 + "WHERE\n"
                                 + "   bp.PRODUCT_NAME IS NOT NULL\n"
                                 + "AND b.CREATE_TIME > ?";
        String queryUpdatedSql = "SELECT\n"
                                 + "   b.BIDER_ID AS supplierId,\n"
                                 + "   b.BIDER_NAME AS supplierName,\n"
                                 + "   bp.PRODUCT_NAME AS directoryNameAlias,\n"
                                 + "   2 AS supplierDirectoryRel,\n"
                                 + "   b.CREATE_TIME AS createTime\n"
                                 + "FROM\n"
                                 + "   bid b\n"
                                 + "JOIN bid_product bp ON b.ID = bp.BID_ID\n"
                                 + "WHERE\n"
                                 + "   bp.PRODUCT_NAME IS NOT NULL\n"
                                 + "AND b.CREATE_TIME > ?\n"
                                 + "LIMIT ?, ?";
        doSyncUpdatedData(ycDataSource, countUpdatedSql, queryUpdatedSql, lastSyncTime);
        logger.info("同步中标的产品结束");
    }

    /**
     * 同步标王关键字
     *
     * @param lastSyncTime
     */
    private void syncProDataService(Timestamp lastSyncTime) {
        logger.info("同步标王关键字开始");
        String countInsertedSql = "SELECT count(1) FROM user_wfirst_use WHERE CREATE_TIME > ? AND ENABLE_DISABLE = 1";
        String queryInsertedSql = "SELECT\n"
                                  + "   ufu.COMPANY_ID AS supplierId,\n"
                                  + "   w.KEY_WORD AS directoryNameAlias,\n"
                                  + "   3 AS supplierDirectoryRel,\n"
                                  + "   ufu.CREATE_TIME AS createTime,\n"
                                  + "   ufu.UPDATE_TIME AS updateTime\n"
                                  + "FROM\n"
                                  + "   user_wfirst_use ufu\n"
                                  + "LEFT JOIN wfirst w ON ufu.WFIRST_ID = w.ID\n"
                                  + "WHERE ufu.CREATE_TIME > ? AND ufu.ENABLE_DISABLE = 1\n"
                                  + "LIMIT ?, ?";
        doSyncInsertedData(proDataSource, countInsertedSql, queryInsertedSql, lastSyncTime);
        logger.info("同步标王关键字结束");
    }

    /**
     * 同步中心库供应商主营产品
     *
     * @param lastSyncTime
     */
    private void syncCenterDataService(Timestamp lastSyncTime) {
        logger.info("同步中心库供应商主营产品开始");
        String countInsertedSql = "SELECT count(1) FROM t_reg_company WHERE CREATE_DATE > ?";
        String queryInsertedSql = "SELECT\n"
                                  + "   trc.ID AS supplierId,\n"
                                  + "   trc.NAME AS supplierName,\n"
                                  + "   trc.MAIN_PRODUCT AS directoryNameAlias,\n"
                                  + "   4 AS supplierDirectoryRel,\n"
                                  + "   tucs.CORE_SUPPLIER_STATUS AS core,\n"
                                  + "   trc.CREATE_DATE AS createTime,\n"
                                  + "   trc.UPDATE_TIME AS updateTime\n"
                                  + "FROM\n"
                                  + "   t_reg_company trc\n"
                                  + "LEFT JOIN t_uic_company_status tucs ON trc.ID = tucs.COMP_ID\n"
                                  + "WHERE\n"
                                  + "   trc.MAIN_PRODUCT IS NOT NULL AND trc.CREATE_DATE > ? LIMIT ?, ?";
        doSyncInsertedData(centerDataSource, countInsertedSql, queryInsertedSql, lastSyncTime);

        String countUpdatedSql = "SELECT count(1) FROM t_reg_company WHERE UPDATE_TIME > ?";
        String queryUpdatedSql = "SELECT\n"
                                 + "   trc.ID AS supplierId,\n"
                                 + "   trc.NAME AS supplierName,\n"
                                 + "   trc.MAIN_PRODUCT AS directoryNameAlias,\n"
                                 + "   4 AS supplierDirectoryRel,\n"
                                 + "   tucs.CORE_SUPPLIER_STATUS AS core,\n"
                                 + "   trc.CREATE_DATE AS createTime,\n"
                                 + "   trc.UPDATE_TIME AS updateTime\n"
                                 + "FROM\n"
                                 + "   t_reg_company trc\n"
                                 + "LEFT JOIN t_uic_company_status tucs ON trc.ID = tucs.COMP_ID\n"
                                 + "WHERE\n"
                                 + "   trc.MAIN_PRODUCT IS NOT NULL AND trc.UPDATE_TIME > ? LIMIT ?, ?";
        doSyncUpdatedData(centerDataSource, countUpdatedSql, queryUpdatedSql, lastSyncTime);
        logger.info("同步中心库供应商主营产品结束");
    }


    private void doSyncInsertedData(DataSource inputDataSource, String countInsertedSql, String queryInsertedSql, Timestamp createTime) {
        List<Object> params = new ArrayList<>();
        params.add(createTime);
        batchSyncData(inputDataSource, countInsertedSql, queryInsertedSql, params);
    }

    private void doSyncUpdatedData(DataSource inputDataSource, String countUpdatedSql, String queryUpdatedSql, Timestamp updateTime) {
        List<Object> params = new ArrayList<>();
        params.add(updateTime);
        batchSyncData(inputDataSource, countUpdatedSql, queryUpdatedSql, params);
    }


    private void batchSyncData(final DataSource inputDataSource, String countSql, final String querySql, final List<Object> params) {
        long count = DBUtil.count(inputDataSource, countSql, params);
        logger.debug("执行countSql : {}, params : {}，共{}条", countSql, params, count);
        if (count > 0) {
            for (long i = 0; i < count; ) {
                final long finalI = i;
                try {
                    semaphore.acquire();
                    executorService.execute(new Runnable() {
                        @Override
                        public void run() {
                            doBatchSyncData(inputDataSource, querySql, params, finalI);
                        }
                    });
                    i += pageSize;
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void doBatchSyncData(DataSource inputDataSource, String querySql, List<Object> params, long i) {
        try {
            List<Object> paramsToUse = appendToParams(params, i);
            List<Map<String, Object>> results = DBUtil.query(inputDataSource, querySql, paramsToUse);
            logger.debug("执行querySql : {}, params : {}，共{}条", querySql, paramsToUse, results.size());
            List<Map<String, Object>> resultsToExecute = new ArrayList<>();

            for (Map<String, Object> result : results) {
                // note:将供应商的产品转换为小写处理es查询时大小写敏感问题
                String directoryName = String.valueOf(result.get(DIRECTORY_NAME)).toLowerCase();
                Long supplierId = Long.valueOf(result.get(SUPPLIER_ID).toString());
                Integer supplierDirectoryRelation = Integer.valueOf(result.get(SUPPLIER_DIRECTORY_REL).toString());

                Map<String, Map<String, Object>> directoryNameMap = queryDirectoryNamesBySupplierId(supplierId);
                String[] changedDirectoryNames = splitDirectoryName(directoryName);
                for (String name : changedDirectoryNames) {
                    if (!StringUtils.isEmpty(name)) {
                        Integer oldSupplierDirectoryRelation = getOldSupplierDirectoryRelation(directoryNameMap, name);
                        // 如果不存在，则直接插入
                        if (oldSupplierDirectoryRelation == null) {
                            result.put(ID, generateSupplierProductId(result));
                            resultsToExecute.add(refresh(result, name));
                        } else {
                            // 如果优先级高，则更新
                            if (supplierDirectoryRelation < oldSupplierDirectoryRelation) {
                                result.put(ID, directoryNameMap.get(name).get(ID));
                                resultsToExecute.add(refresh(result, name));
                            }
                        }
                    }
                }
            }

            batchExecute(resultsToExecute);
        } finally {
            semaphore.release();
        }
    }

    private String generateSupplierProductId(Map<String, Object> result) {
        Long supplierId = (Long) result.get(SUPPLIER_ID);
        String directoryName = String.valueOf(result.get(DIRECTORY_NAME));
        if (supplierId == null) {
            throw new RuntimeException("供应商产品ID生成失败，原因：供应商ID为空!");
        }
        if (StringUtils.isEmpty(directoryName)) {
            throw new RuntimeException("供应商产品ID生成失败，原因：directoryName为null!");
        }

        return DigestUtils.md5DigestAsHex((supplierId + "_" + directoryName).getBytes());
    }

    /**
     * 替换供应产品名称
     *
     * @param result
     * @param directoryName
     * @return
     */
    private Map<String, Object> refresh(Map<String, Object> result, String directoryName) {
        Map<String, Object> resultToUse = new HashMap<>(result);
        // 将多个拼接在一起的采购品分割为单个采购品
        resultToUse.remove(DIRECTORY_NAME);
        resultToUse.put(DIRECTORY_NAME, directoryName.trim());
        // 将supplierId转为string
        resultToUse.put(SUPPLIER_ID, String.valueOf(resultToUse.get(SUPPLIER_ID)));
        resultToUse.put(SyncTimeUtil.SYNC_TIME, SyncTimeUtil.getCurrentDate());
        return resultToUse;
    }

    private Integer getOldSupplierDirectoryRelation(Map<String, Map<String, Object>> directoryNameMap, String name) {
        if (directoryNameMap == null) {
            return null;
        } else {
            Map<String, Object> map = directoryNameMap.get(name);
            if (map == null) {
                return null;
            } else {
                return (Integer) map.get(SUPPLIER_DIRECTORY_REL);
            }
        }
    }

    private Map<String, Map<String, Object>> queryDirectoryNamesBySupplierId(Long supplierId) {
        SearchResponse response = elasticClient.getTransportClient()
                .prepareSearch(elasticClient.getProperties().getProperty("cluster.index"))
                .setTypes(elasticClient.getProperties().getProperty("cluster.type.supplier_product"))
                .setQuery(QueryBuilders.termQuery(SUPPLIER_ID, supplierId))
                .execute().actionGet();
        SearchHits hits = response.getHits();
        Map<String, Map<String, Object>> directoryNameMap = new HashMap<>();
        for (SearchHit searchHit : hits.hits()) {
            Map<String, Object> source = searchHit.getSource();
            directoryNameMap.put(String.valueOf(source.get(DIRECTORY_NAME)), source);
        }
        return directoryNameMap;
    }

    private String[] splitDirectoryName(String directoryName) {
        return directoryName.replaceAll("[,;.，。、\\s\\t]", ",").split(",");
    }

    private List<Object> appendToParams(List<Object> params, long i) {
        List<Object> paramsToUse = new ArrayList<>(params);
        paramsToUse.add(i);
        paramsToUse.add(pageSize);
        return paramsToUse;
    }

    private void batchExecute(List<Map<String, Object>> resultsToUpdate) {
        if (!CollectionUtils.isEmpty(resultsToUpdate)) {
            BulkRequestBuilder bulkRequest = elasticClient.getTransportClient().prepareBulk();
            for (Map<String, Object> result : resultsToUpdate) {
                bulkRequest.add(elasticClient.getTransportClient()
                                        .prepareIndex(elasticClient.getProperties().getProperty("cluster.index"),
                                                      elasticClient.getProperties().getProperty("cluster.type.supplier_product"),
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


//    @Override
//    public void afterPropertiesSet() throws Exception {
//        execute();
//    }
}
