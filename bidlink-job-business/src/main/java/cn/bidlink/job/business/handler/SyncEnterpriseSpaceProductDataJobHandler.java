package cn.bidlink.job.business.handler;

import cn.bidlink.job.common.constant.BusinessConstant;
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
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.joda.time.DateTime;
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
 * @author <a href="mailto:zhihuizhou@ebnew.com">zhouzhihui</a>
 * @version Ver 1.0
 * @description:
 * @Date 2018/2/27
 */
@JobHander(value = "syncEnterpriseSpaceDataJobHandler")
@Service
public class SyncEnterpriseSpaceProductDataJobHandler extends JobHandler /*implements InitializingBean*/ {

    private Logger logger = LoggerFactory.getLogger(SyncEnterpriseSpaceProductDataJobHandler.class);

    @Autowired
    @Qualifier("enterpriseSpaceDataSource")
    private DataSource enterpriseSpaceDataSource;

    @Autowired
    @Qualifier("uniregDataSource")
    protected DataSource uniregDataSource;

    @Autowired
    private ElasticClient elasticClient;

    @Autowired
    @Qualifier(value = "proDataSource")
    private DataSource proDataSource;

    private String  ID                        = "id";
    private String  CORE                      = "core";
    private String  CORE_STATUS               = "coreStatus";
    private String  COMPANY_ID                = "companyId";
    private String  PRODUCT_NAME              = "productName";
    private String  PRODUCT_NAME_NOT_ANALYZED = "productNameNotAnalyzed";
    private String  ZONE_STR                  = "zoneStr";
    private String  ZONE_STR_NOT_ANALYZED     = "zoneStrNotAnalyzed";
    private String  WFIRST_STATUS             = "wfirstStatus";
    private Integer BINDING_WFIRST            = 2;  //产品绑定了标王
    private Integer NO_WFIRST                 = 1;  //产品没绑定标王

    @Override
    public ReturnT<String> execute(String... strings) throws Exception {
        SyncTimeUtil.setCurrentDate();
        logger.info("同步企业空间产品开始");
        syncEnterpriseSpaceData();
        logger.info("同步企业空间产品结束");
        return ReturnT.SUCCESS;
    }

    private void syncEnterpriseSpaceData() {
        Timestamp lastSyncTime = ElasticClientUtil.getMaxTimestamp(elasticClient,
                "cluster.index",
                "cluster.type.enterprise_space_product",
                null);
        logger.info("企业空间同步lastTime:" + new DateTime(lastSyncTime).toString("yyyy-MM-dd HH:mm:ss") + "\n" +
                ", syncTime:" + new DateTime(SyncTimeUtil.getCurrentDate()).toString("yyyy-MM-dd HH:mm:ss"));
//        Timestamp lastSyncTime = new Timestamp(0);
        syncCreateEnterpriseSpaceService(lastSyncTime);
        syncUpdateEnterpriseSpaceService(lastSyncTime);
        // 添加产品是否绑定了标王 2:绑定 1:没绑定
        fixedProductWfirstStatus();
    }

    private void fixedProductWfirstStatus() {
        Properties properties = elasticClient.getProperties();
        // 回滚标王数据
        SearchResponse wfirstResponse = elasticClient.getTransportClient().prepareSearch(properties.getProperty("cluster.index"))
                .setTypes(properties.getProperty("cluster.type.supplier_product"))
                .setQuery(QueryBuilders.termQuery("supplierDirectoryRel", 3))
                .setScroll(new TimeValue(60000))
                .setSize(pageSize)
                .execute().actionGet();
        do {
            SearchHits wfirstHits = wfirstResponse.getHits();
            if (wfirstHits.getTotalHits() > 0) {
                List<Map<String, Object>> resultFromEs = new ArrayList<>();
                for (SearchHit hit : wfirstHits.getHits()) {
                    Map<String, Object> source = hit.getSource();
                    HashMap<String, Object> map = new HashMap<>(2);
                    map.put("supplierId", source.get("supplierId"));
                    map.put("directoryName", source.get("directoryName"));
                    resultFromEs.add(map);
                }

                BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
                for (Map<String, Object> condition : resultFromEs) {
                    BoolQueryBuilder must = QueryBuilders.boolQuery().must(QueryBuilders.termQuery("companyId", condition.get("supplierId")))
                            .must(QueryBuilders.wildcardQuery("productName", "*" + condition.get("directoryName") + "*"));
                    boolQuery.should(must);
                }

                // 回滚包含标王的产品数据,更新产品
                SearchResponse productResponse = elasticClient.getTransportClient().prepareSearch(properties.getProperty("cluster.index"))
                        .setTypes(properties.getProperty("cluster.type.enterprise_space_product"))
                        .setScroll(new TimeValue(60000))
                        .setQuery(boolQuery)
                        .setSize(pageSize)
                        .execute().actionGet();
                SearchHits productHits = productResponse.getHits();
                do {
                    if (productHits.getTotalHits() > 0) {
                        List<Map<String, Object>> mapList = Arrays.stream(productHits.getHits()).map(searchHit -> {
                            Map<String, Object> source = searchHit.getSource();
                            source.put(WFIRST_STATUS, BINDING_WFIRST);
                            return source;
                        }).collect(Collectors.toList());
                        batchUpdate(mapList);
                        productResponse = elasticClient.getTransportClient().prepareSearchScroll(productResponse.getScrollId())
                                .setScroll(new TimeValue(60000))
                                .execute().actionGet();
                    }
                } while (productResponse.getHits().getHits().length != 0);
//                // 添加产品标王状态
//                appendProductWfirstStatus(resultFromEs, productIds);
//                batchInsert(resultFromEs);
                wfirstResponse = elasticClient.getTransportClient().prepareSearchScroll(wfirstResponse.getScrollId())
                        .setScroll(new TimeValue(60000))
                        .execute().actionGet();
            }
        } while (wfirstResponse.getHits().getHits().length != 0);
    }

    private void batchUpdate(List<Map<String, Object>> mapList) {
        if (!CollectionUtils.isEmpty(mapList)) {
            BulkRequestBuilder prepareBulk = elasticClient.getTransportClient().prepareBulk();
            Properties properties = elasticClient.getProperties();
            for (Map<String, Object> map : mapList) {
                prepareBulk.add(elasticClient.getTransportClient().prepareUpdate(
                        properties.getProperty("cluster.index"), properties.getProperty("cluster.type.enterprise_space_product"),
                        String.valueOf(map.get(ID))
                ).setDoc(JSON.toJSONString(map, new ValueFilter() {
                    @Override
                    public Object process(Object object, String propertyName, Object propertyValue) {
                        if (propertyValue instanceof Date) {
                            return SyncTimeUtil.toDateString(propertyValue);
                        }
                        return propertyValue;
                    }
                })));
            }
            BulkResponse response = prepareBulk.execute().actionGet();
            if (response.hasFailures()) {
                logger.error("更新产品状态失败,错误信息:{}", response.buildFailureMessage());
            }
        }
    }

    private void appendProductWfirstStatus(List<Map<String, Object>> resultFromEs, List<Long> productIds) {
        if (!CollectionUtils.isEmpty(productIds)) {
            String querySqlTempalte = "SELECT\n" +
                    "\tPRODUCT_ID AS productId \n" +
                    "FROM\n" +
                    "\t`user_wfirst_use` \n" +
                    "WHERE\n" +
                    "\tPRODUCT_ID IN (%s) and state = 1 and enable_disable = 1";
            String querySql = String.format(querySqlTempalte, StringUtils.collectionToCommaDelimitedString(productIds));
            List<Long> wfirstProductIds = DBUtil.query(proDataSource, querySql, null, new DBUtil.ResultSetCallback<List<Long>>() {
                @Override
                public List<Long> execute(ResultSet resultSet) throws SQLException {
                    ArrayList<Long> productIdsHaveWfirst = new ArrayList<>();
                    while (resultSet.next()) {
                        productIdsHaveWfirst.add(resultSet.getLong(1));
                    }
                    return productIdsHaveWfirst;
                }
            });
            logger.info("查询产品绑定了标王的产品,querySql:{}, 总共{}条", querySql, wfirstProductIds.size());
            for (Map<String, Object> result : resultFromEs) {
                if (!Objects.isNull(result.get(ID)) && wfirstProductIds.contains(Long.valueOf(String.valueOf(result.get(ID))))) {
                    result.put(WFIRST_STATUS, BINDING_WFIRST);
                } else {
                    result.put(WFIRST_STATUS, NO_WFIRST);
                }
            }
        }
    }

    /**
     * 同步更新企业空间产品
     *
     * @param lastTime
     */
    private void syncUpdateEnterpriseSpaceService(Timestamp lastTime) {
        String countSql = "SELECT\n" +
                "\tcount(1)\n" +
                "FROM\n" +
                "\tspace_product" +
                "\tWHERE update_time > ? \n";
        String querySql = "SELECT\n" +
                "\tid,\n" +
                "\tCOMPANYID AS companyId,\n" +
                "\tPICFILE AS picFile,\n" +
                "\ttitle AS productName,\n" +
                "\tCOMPANYNAME AS companyName,\n" +
                "\tSTATE AS state,\n" +
                "\tZONESTR AS zoneStrNotAnalyzed,\n" +
                "\tcreate_time AS createTime,\n" +
                "\tNOTICESTATE AS noticeState,\n" +
                "\tis_del AS isDel,\n" +
                "\tspec AS spec,\n" +
                "\ttrademark AS trademark,\n" +
                "\tmodel AS model,\n" +
                "\tZONESTR AS zoneStr\n" +
                "FROM\n" +
                "\tspace_product \n" +
                "\tWHERE update_time > ? \n" +
                "\tLIMIT ?,?";
        ArrayList<Object> params = new ArrayList<>();
        params.add(lastTime);
        doSyncEnterpriseSpaceService(countSql, querySql, params);
    }

    /**
     * 同步插入企业空间产品
     *
     * @param lastTime
     */
    private void syncCreateEnterpriseSpaceService(Date lastTime) {
        String countSql = "SELECT\n" +
                "\tcount(1)\n" +
                "FROM\n" +
                "\tspace_product" +
                "\tWHERE create_time > ? \n";
        String querySql = "SELECT\n" +
                "\tid,\n" +
                "\tCOMPANYID AS companyId,\n" +
                "\tPICFILE AS picFile,\n" +
                "\ttitle AS productName,\n" +
                "\tCOMPANYNAME AS companyName,\n" +
                "\tSTATE AS state,\n" +
                "\tZONESTR AS zoneStrNotAnalyzed,\n" +
                "\tcreate_time AS createTime,\n" +
                "\tNOTICESTATE AS noticeState,\n" +
                "\tis_del AS isDel,\n" +
                "\tspec AS spec,\n" +
                "\ttrademark AS trademark,\n" +
                "\tmodel AS model,\n" +
                "\tZONESTR AS zoneStr\n" +
                "FROM\n" +
                "\tspace_product \n" +
                "\tWHERE create_time > ? \n" +
                "\tLIMIT ?,?";
        ArrayList<Object> params = new ArrayList<>();
        params.add(lastTime);
        doSyncEnterpriseSpaceService(countSql, querySql, params);
    }

    private void doSyncEnterpriseSpaceService(String countSql, String querySql, ArrayList<Object> params) {
        long count = DBUtil.count(enterpriseSpaceDataSource, countSql, params);
        logger.debug("执行countSql:{}, params:{} , 共{}条", countSql, params, count);
        for (long i = 0; i < count; i += pageSize) {
            List<Object> paramsToUse = appendToParams(params, i);
            List<Map<String, Object>> products = DBUtil.query(enterpriseSpaceDataSource, querySql, paramsToUse);

            // 添加供应商状态 t_reg_company中core_status字段
            appendSupplierStatus(products);

            // 添加同步时间和字段类型转换
            for (Map<String, Object> product : products) {
                refresh(product);
            }
            // 插入es中
            batchInsert(products);
        }
    }

    /**
     * 添加供应商状态 core: 0表示非核心供    1:核心供
     *
     * @param mapList
     */
    private void appendSupplierStatus(List<Map<String, Object>> mapList) {
        HashSet<Long> companyIds = new HashSet<>();
        for (Map<String, Object> map : mapList) {
            companyIds.add(((Long) map.get(COMPANY_ID)));
        }

        String querySqlTemplate = "SELECT\n" +
                "\tid AS companyId,\n" +
                "\tcore_status AS coreStatus \n" +
                "FROM\n" +
                "\t`t_reg_company` \n" +
                "WHERE\n" +
                "\tid IN (%s)";
        String querySql = String.format(querySqlTemplate, StringUtils.collectionToCommaDelimitedString(companyIds));
        Map<Long, Integer> coreMap = DBUtil.query(uniregDataSource, querySql, null, new DBUtil.ResultSetCallback<Map<Long, Integer>>() {
            @Override
            public Map<Long, Integer> execute(ResultSet resultSet) throws SQLException {
                HashMap<Long, Integer> map = new HashMap<>();
                while (resultSet.next()) {
                    String coreStatus = resultSet.getString(CORE_STATUS);
                    map.put(resultSet.getLong(COMPANY_ID), coreStatus == null ? 0 : Integer.valueOf(coreStatus.substring(0, 1)));
                }
                return map;
            }
        });

        for (Map<String, Object> map : mapList) {
            map.put(CORE, coreMap.get(map.get(COMPANY_ID)));
        }
    }

    /**
     * 添加同步时间字段
     *
     * @param product
     */
    private void refresh(Map<String, Object> product) {
        product.put(ID, String.valueOf(product.get(ID)));
        product.put(COMPANY_ID, String.valueOf(product.get(COMPANY_ID)));
        product.put(ZONE_STR_NOT_ANALYZED, product.get(ZONE_STR));
        product.put(PRODUCT_NAME_NOT_ANALYZED, product.get(PRODUCT_NAME));
        product.put(SyncTimeUtil.SYNC_TIME, SyncTimeUtil.getCurrentDate());
        // 默认没绑定标王状态
        product.put(WFIRST_STATUS, NO_WFIRST);
        //添加平台来源
        product.put(BusinessConstant.PLATFORM_SOURCE_KEY, BusinessConstant.IXIETONG_SOURCE);
    }

    /**
     * 批量插入es
     *
     * @param products
     */
    private void batchInsert(List<Map<String, Object>> products) {
//        System.out.println(products);
        if (!CollectionUtils.isEmpty(products)) {
            BulkRequestBuilder bulkRequest = elasticClient.getTransportClient().prepareBulk();
            for (Map<String, Object> product : products) {
                bulkRequest.add(elasticClient.getTransportClient()
                        .prepareIndex(elasticClient.getProperties().getProperty("cluster.index"),
                                elasticClient.getProperties().getProperty("cluster.type.enterprise_space_product"),
                                String.valueOf(product.get(ID)))
                        .setSource(JSON.toJSONString(product, new ValueFilter() {
                            @Override
                            public Object process(Object object, String propertyName, Object propertyValue) {
                                if (propertyValue instanceof java.util.Date) {
                                    // 是date类型按指定日期格式转换
                                    return new DateTime(propertyValue).toString(SyncTimeUtil.DATE_TIME_PATTERN);
                                } else {

                                    return propertyValue;
                                }
                            }
                        })));
            }
            BulkResponse response = bulkRequest.execute().actionGet();
            // 是否失败
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
