package cn.bidlink.job.ycsearch.handler.projectexpress;

import cn.bidlink.job.common.constant.BusinessConstant;
import cn.bidlink.job.common.es.ElasticClient;
import cn.bidlink.job.common.utils.DBUtil;
import cn.bidlink.job.common.utils.SyncTimeUtil;
import cn.bidlink.job.ycsearch.handler.JobHandler;
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
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author <a href="mailto:zhihuizhou@ebnew.com">wisdom</a>
 * @version Ver 1.0
 * @description:悦采供应商交易数据统计,给项目直通车查询
 * @Date 2018/11/2
 */
@Service
@JobHander(value = "syncYcSupplierProjectDataJobHandler")
public class SyncYcSupplierProjectDataJobHandler extends JobHandler /*implements InitializingBean*/ {

    private Logger logger = LoggerFactory.getLogger(SyncYcSupplierProjectDataJobHandler.class);

    @Autowired
    private ElasticClient elasticClient;

    @Autowired
    @Qualifier(value = "ycDataSource")
    private DataSource ycDataSource;

    private   String ID                         = "id";
    private   String COMPANY_NAME               = "companyName";
    protected String AREA_STR                   = "areaStr";
    protected String MAIN_PRODUCT               = "mainProduct";
    private   String TOTAL_COOPERATED_PURCHASER = "totalCooperatedPurchaser";
    private   String TOTAL_DEAL_PRICE           = "totalDealPrice";

    @Override
    public ReturnT<String> execute(String... strings) throws Exception {
        SyncTimeUtil.setCurrentDate();
        logger.info("0.开始同步悦采供应商成交量和合作供应商统计");
        syncYcSupplierProjectData();
        logger.info("1.结束同步悦采供应商成交量和合作供应商统计");
        return ReturnT.SUCCESS;
    }

    private void syncYcSupplierProjectData() {
        Properties properties = elasticClient.getProperties();
        SearchResponse response = elasticClient.getTransportClient().prepareSearch(properties.getProperty("cluster.index"))
                .setTypes(properties.getProperty("cluster.type.supplier"))
                .setQuery(QueryBuilders.termQuery(BusinessConstant.WEB_TYPE, BusinessConstant.YUECAI))
                .setScroll(new TimeValue(60000))
                .setSize(pageSize)
                .execute().actionGet();
        do {
            SearchHits hits = response.getHits();
            if (hits.getTotalHits() > 0) {
                List<Long> supplierIds = new ArrayList<>();
                List<Map<String, Object>> resultFromEs = new ArrayList<>();
                for (SearchHit hit : hits.getHits()) {
                    Map<String, Object> source = hit.getSource();
                    supplierIds.add(Long.valueOf(String.valueOf(source.get("id"))));
                    resultFromEs.add(source);
                }
                String supplierIdsStr = StringUtils.collectionToCommaDelimitedString(supplierIds);
                // cooperationPurchaserCount 合作采购商数量
                this.appendCooperationPurchaser(resultFromEs, supplierIdsStr);

                // 供应商采购项目和招标项目成交总金额
                this.appendTradingVolume(resultFromEs, supplierIdsStr);
                // 处理字段
                List<Map<String, Object>> mapList = this.handlerColumn(resultFromEs);
                batchInsert(mapList);
                response = elasticClient.getTransportClient().prepareSearchScroll(response.getScrollId())
                        .setScroll(new TimeValue(60000))
                        .execute().actionGet();
            }
        } while (response.getHits().getHits().length != 0);
    }

    private void batchInsert(List<Map<String, Object>> mapList) {
        if (!CollectionUtils.isEmpty(mapList)) {
            Properties properties = elasticClient.getProperties();
            BulkRequestBuilder requestBuilder = elasticClient.getTransportClient().prepareBulk();
            for (Map<String, Object> map : mapList) {
                requestBuilder.add(elasticClient.getTransportClient().prepareIndex(properties.getProperty("cluster.express_index"),
                        properties.getProperty("cluster.type.supplier_express"))
                        .setId(String.valueOf(map.get(ID)))
                        .setSource(JSON.toJSONString(map, new ValueFilter() {
                            @Override
                            public Object process(Object object, String propertyName, Object propertyValue) {
                                if (propertyValue instanceof Date) {
                                    return SyncTimeUtil.toDateString(propertyValue);
                                }
                                return propertyValue;
                            }
                        }))
                );
            }
            BulkResponse response = requestBuilder.execute().actionGet();
            if (response.hasFailures()) {
                logger.error("悦采供应商数据插入失败,异常信息:{}", response.buildFailureMessage());
            }
        }
    }

    private List<Map<String, Object>> handlerColumn(List<Map<String, Object>> resultFromEs) {
        return resultFromEs.stream().map(map -> {
            Map<String, Object> result = new HashMap<>(6);
            result.put(ID, map.get(ID));
            result.put(COMPANY_NAME, map.get(COMPANY_NAME));
            result.put(TOTAL_COOPERATED_PURCHASER, map.get(TOTAL_COOPERATED_PURCHASER));
            result.put(AREA_STR, map.get(AREA_STR));
            result.put(MAIN_PRODUCT, map.get(MAIN_PRODUCT));
            result.put(TOTAL_DEAL_PRICE, map.get(TOTAL_DEAL_PRICE));
            return result;
        }).collect(Collectors.toList());
    }

    private void appendTradingVolume(List<Map<String, Object>> resultFromEs, String supplierIdsStr) {
        if (!StringUtils.isEmpty(supplierIdsStr)) {
            // 1.采购项目
            String querySqlTemplate = "SELECT\n" +
                    "\tsum( deal_total_price ) ,\n" +
                    "\tsupplier_id\n" +
                    "FROM\n" +
                    "\t`bmpfjz_supplier_project_bid` \n" +
                    "WHERE\n" +
                    "\tsupplier_bid_status = 6 \n" +
                    "\tand supplier_id in (%s)\n" +
                    "GROUP BY\n" +
                    "\tsupplier_id";
            Map<String, Long> supplierPurchaseVolume = this.getSupplierStatMap(ycDataSource, querySqlTemplate, supplierIdsStr);
            for (Map<String, Object> resultMap : resultFromEs) {
                Long purchaseVolume = supplierPurchaseVolume.get(Long.valueOf(String.valueOf(resultMap.get(ID))));
                resultMap.put(TOTAL_DEAL_PRICE, purchaseVolume == null ? 0 : purchaseVolume);
            }

            // 2.招标项目
        }
    }

    private void appendCooperationPurchaser(List<Map<String, Object>> resultFromEs, String supplierIds) {
        if (!StringUtils.isEmpty(supplierIds)) {
            String queryCooperatedPurchaserSqlTemplate = "select\n"
                    + "   count(1) AS totalCooperatedPurchaser,\n"
                    + "   supplier_id AS supplierId\n"
                    + "from\n"
                    + "   bsm_company_supplier_apply\n"
                    + "   WHERE supplier_status in (2,3,4) AND supplier_id in (%s)\n"
                    + "GROUP BY\n"
                    + "   supplier_id\n";
            Map<String, Long> supplierStat = this.getSupplierStatMap(ycDataSource, queryCooperatedPurchaserSqlTemplate, supplierIds);
            for (Map<String, Object> source : resultFromEs) {
                Object value = supplierStat.get(source.get(ID));
                source.put(TOTAL_COOPERATED_PURCHASER, (value == null ? 0 : value));
            }
        }
    }

    private Map<String, Long> getSupplierStatMap(DataSource dataSource, String querySqlTemplate, String supplierIds) {
        String querySql = String.format(querySqlTemplate, supplierIds);
        return DBUtil.query(dataSource, querySql, null, new DBUtil.ResultSetCallback<Map<String, Long>>() {
            @Override
            public Map<String, Long> execute(ResultSet resultSet) throws SQLException {
                Map<String, Long> map = new HashMap<>();
                while (resultSet.next()) {
                    map.put(String.valueOf(resultSet.getLong(2)), resultSet.getLong(1));
                }
                return map;
            }
        });
    }

//    @Override
//    public void afterPropertiesSet() throws Exception {
//        execute();
//    }
}
