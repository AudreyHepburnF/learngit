package cn.bidlink.job.product.handler;

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
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import java.util.*;

/**
 * @author : <a href="mailto:zikaifeng@ebnew.com">冯子恺</a>
 * @version : Ver 1.0
 * @description :
 * @date : 2017/9/14
 */
@JobHander(value = "syncProductReportJobHandler")
@Service
public class SyncProductReportJobHandler extends IJobHandler /*implements InitializingBean*/ {
    private Logger logger = LoggerFactory.getLogger(SyncProductReportJobHandler.class);

    @Autowired
    private ElasticClient elasticClient;

    @Autowired
    @Qualifier("omsDataSource")
    private DataSource omsDataSource;

    @Value("${pageSize:200}")
    private int pageSize;

    @Value("${productCodeType}")
    private String productCodeType;

    // 两位有效数字，四舍五入
    private DecimalFormat format = new DecimalFormat("0.00");
    // 折扣除数
    private BigDecimal TEN = new BigDecimal(10);

    // 已上架
    private int ON_SALES  = 1;
    // 下架
    private int OFF_SALES = 2;

    private String ID              = "id";
    private String PRICE           = "price";
    private String DISCOUNT        = "discount";
    private String DISCOUNTED_PRICE  = "discountedPrice";
    private String PRODUCT_ID      = "productId";
    private String IS_SALES        = "isSales";
    private String DOC_ABSTRACT    = "docAbstract";
    private String INDUSTRY        = "industry";
    private String INDUSTRY_CODE   = "industryCode";
    private String KEYWORD         = "keyword";
    private String PUBLISH_DATE    = "publishDate";
    private String MONITOR_PRODUCT = "monitorProduct";

    // 报告的扩展字段
    private List<String> columns = Arrays.asList(DOC_ABSTRACT, INDUSTRY, INDUSTRY_CODE, KEYWORD, PUBLISH_DATE, MONITOR_PRODUCT);


    @Override
    public ReturnT<String> execute(String... strings) throws Exception {
        SyncTimeUtil.setCurrentDate();
        logger.info("同步行情产品报告开始");
        synProductReport();
        logger.info("同步行情产品报告结束");
        return ReturnT.SUCCESS;
    }

    private void synProductReport() {
        Timestamp lastSyncTime = ElasticClientUtil.getMaxTimestamp(elasticClient, "cluster.index", "cluster.type.product_report", null);
        logger.debug("同步行情产品报告lastSyncTime：" + new DateTime(lastSyncTime).toString("yyyy-MM-dd HH:mm:ss"));
        syncCreatedProductReport(lastSyncTime);
        syncUpdatedProductReport(lastSyncTime);
    }

    private void syncCreatedProductReport(Timestamp lastSyncTime) {
        String countCreatedProductSql = "SELECT\n"
                                        + "   count(1)\n"
                                        + "FROM\n"
                                        + "   product p\n"
                                        + "WHERE\n"
                                        + "   p.product_type_code = ?\n"
                                        + "AND p.is_sales > 0\n"
                                        + "AND p.create_time >= ?";
        String queryCreatedProductSql = "SELECT\n"
                                        + "   p.id,\n"
                                        + "   p.`name`,\n"
                                        + "   p.`price`,\n"
                                        + "   p.`discount`,\n"
                                        + "   p.product_code AS productCode,\n"
                                        + "   p.product_type_code AS productTypeCode,\n"
                                        + "   p.is_sales AS isSales\n"
                                        + "FROM\n"
                                        + "   product p\n"
                                        + "WHERE\n"
                                        + "   p.product_type_code = ?\n"
                                        + "AND p.is_sales > 0\n"
                                        + "AND p.create_time >= ?\n"
                                        + "LIMIT ?, ?";

        List<Object> params = new ArrayList<>();
        params.add(productCodeType);
        params.add(lastSyncTime);
        doSyncProjectDataService(countCreatedProductSql, queryCreatedProductSql, params);
    }

    /**
     * 产品报告重新发布会导致已上架的产品变为未上架，并更新时间字段
     *
     * @param lastSyncTime
     */
    private void syncUpdatedProductReport(Timestamp lastSyncTime) {
        String countUpdatedProductSql = "SELECT\n"
                                        + "   count(1)\n"
                                        + "FROM\n"
                                        + "   product p\n"
                                        + "WHERE\n"
                                        + "   p.product_type_code = ?\n"
                                        + "AND p.is_sales >= 0\n"
                                        + "AND p.update_time >= ?";
        String queryUpdatedProductSql = "SELECT\n"
                                        + "   p.id,\n"
                                        + "   p.`name`,\n"
                                        + "   p.`price`,\n"
                                        + "   p.`discount`,\n"
                                        + "   p.product_code AS productCode,\n"
                                        + "   p.product_type_code AS productTypeCode,\n"
                                        + "   p.is_sales AS isSales\n"
                                        + "FROM\n"
                                        + "   product p\n"
                                        + "WHERE\n"
                                        + "   p.product_type_code = ?\n"
                                        + "AND p.is_sales >= 0\n"
                                        + "AND p.update_time >= ?\n"
                                        + "LIMIT ?, ?";

        List<Object> params = new ArrayList<>();
        params.add(productCodeType);
        params.add(lastSyncTime);
        doSyncProjectDataService(countUpdatedProductSql, queryUpdatedProductSql, params);
    }

    protected void doSyncProjectDataService(String countSql, String querySql, List<Object> params) {
        long count = DBUtil.count(omsDataSource, countSql, params);
        logger.debug("执行countSql : {}, params : {}，共{}条", countSql, params, count);
        if (count > 0) {
            for (long i = 0; i < count; ) {
                List<Object> paramsToUse = appendToParams(params, i);
                // 查出符合条件的产品报告
                List<Map<String, Object>> products = DBUtil.query(omsDataSource, querySql, paramsToUse);
                logger.debug("执行querySql : {}, params : {}，共{}条", querySql, paramsToUse, products.size());
                // 产品id
                Set<Long> onSalesProductIds = new HashSet<>();
                for (Map<String, Object> product : products) {
                    Integer isSales = (Integer) product.get(IS_SALES);
                    if (isSales != null) {
                        Long productId = (Long) product.get(ID);
                        if (isSales == ON_SALES) {
                            onSalesProductIds.add(productId);
                        }
                        // 添加同步时间
                        refresh(product);
                    }
                }

                // 添加产品信息
                appendProduct(products, onSalesProductIds);
                // 插入到es
                batchInsert(products);
                i += pageSize;
            }
        }
    }

    private void appendProduct(List<Map<String, Object>> products, Set<Long> productIds) {
        String queryProductSqlTemplate = "SELECT\n"
                                         + "   product_id AS productId,\n"
                                         + "   `key`,\n"
                                         + "   `value`\n"
                                         + "FROM\n"
                                         + "   product_attribute\n"
                                         + "WHERE\n"
                                         + "   product_id IN (%s);";
        if (!CollectionUtils.isEmpty(productIds)) {
            String queryProductSql = String.format(queryProductSqlTemplate, StringUtils.collectionToCommaDelimitedString(productIds));
            Map<Long, Map<String, Object>> productAttributeMap = DBUtil.query(omsDataSource, queryProductSql, null, new DBUtil.ResultSetCallback<Map<Long, Map<String, Object>>>() {
                @Override
                public Map<Long, Map<String, Object>> execute(ResultSet resultSet) throws SQLException {
                    Map<Long, Map<String, Object>> productAttributeMap = new HashMap<>();
                    while (resultSet.next()) {
                        long productId = resultSet.getLong(PRODUCT_ID);
                        String key = resultSet.getString("key");
                        String value = resultSet.getString("value");
                        if (columns.contains(key)) {
                            Map<String, Object> attributeMap = productAttributeMap.get(productId);
                            if (attributeMap == null) {
                                attributeMap = new HashMap<>();
                                productAttributeMap.put(productId, attributeMap);
                            }
                            attributeMap.put(key, value);
                        }
                    }
                    return productAttributeMap;
                }
            });

            for (Map<String, Object> product : products) {
                Integer isSales = (Integer) product.get(IS_SALES);
                if (isSales != null && isSales == ON_SALES) {
                    Long productId = Long.valueOf(String.valueOf(product.get(ID)));
                    Map<String, Object> productAttribute = productAttributeMap.get(productId);
                    if (productAttribute != null) {
                        product.put(DOC_ABSTRACT, productAttribute.get(DOC_ABSTRACT));
                        product.put(INDUSTRY, productAttribute.get(INDUSTRY));
                        product.put(INDUSTRY_CODE, productAttribute.get(INDUSTRY_CODE));
                        product.put(KEYWORD, productAttribute.get(KEYWORD));
                        product.put(PUBLISH_DATE, productAttribute.get(PUBLISH_DATE));
                        product.put(MONITOR_PRODUCT, productAttribute.get(MONITOR_PRODUCT));
                    }
                }
            }
        }
    }

    protected List<Object> appendToParams(List<Object> params, long i) {
        List<Object> paramsToUse = new ArrayList<>(params);
        paramsToUse.add(i);
        paramsToUse.add(pageSize);
        return paramsToUse;
    }

    private void refresh(Map<String, Object> result) {
        result.put(ID, String.valueOf(result.get(ID)));
        result.put(SyncTimeUtil.SYNC_TIME, SyncTimeUtil.getCurrentDate());
        // 添加折扣价
        BigDecimal price = (BigDecimal) result.get(PRICE);
        BigDecimal discount = (BigDecimal) result.get(DISCOUNT);
        String discountedPrice = null;
        if (discount != null) {
            discountedPrice = this.format.format(price.multiply(discount.divide(TEN)));
        }
        result.put(PRICE, String.valueOf(price));
        result.put(DISCOUNTED_PRICE, discountedPrice);
    }

    protected void batchInsert(List<Map<String, Object>> reports) {
//        System.out.println("size : " + reports.size());
//        for (Map<String, Object> map : reports) {
//            System.out.println(map);
//        }
        if (!CollectionUtils.isEmpty(reports)) {
            BulkRequestBuilder bulkRequest = elasticClient.getTransportClient().prepareBulk();
            for (Map<String, Object> result : reports) {
                bulkRequest.add(elasticClient.getTransportClient()
                                        .prepareIndex(elasticClient.getProperties().getProperty("cluster.index"),
                                                      elasticClient.getProperties().getProperty("cluster.type.product_report"),
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
